(ns nrepl.socket
  "Compatibility layer for java.io vs java.nio sockets to allow an
  incremental transition to nio, since the JDK's filesystem sockets
  don't support the java.io socket interface, and we can't use the
  compatibility layer for bidirectional read and write:
  https://bugs.openjdk.java.net/browse/JDK-4509080."
  (:require
   [clojure.java.io :as io]
   [nrepl.misc :refer [log]])
  (:import
   (java.io BufferedInputStream BufferedOutputStream File)
   (java.net InetSocketAddress ProtocolFamily ServerSocket Socket SocketAddress
             StandardProtocolFamily URI)
   (java.nio ByteBuffer)
   (java.nio.file Path)
   (java.nio.channels Channels ClosedChannelException NetworkChannel
                      ServerSocketChannel SocketChannel)))

(def orig-warn-on-reflection *warn-on-reflection*)

(defmacro find-class [full-path]
  `(try
     (Class/forName (name ~full-path))
     (catch ClassNotFoundException ex#
       nil)))

;;; InetSockets (TCP)

(defn inet-socket [bind port]
  (let [port (or port 0)
        addr (fn [^String bind port] (InetSocketAddress. bind (int port)))
        ;; We fallback to 127.0.0.1 instead of to localhost to avoid
        ;; a dependency on the order of ipv4 and ipv6 records for
        ;; localhost in /etc/hosts
        bind (or bind "127.0.0.1")]
    (doto (ServerSocket.)
      (.setReuseAddress true)
      (.bind (addr bind port)))))

;; Unix domain sockets

(def ^Class junixsocket-address-class
  (find-class 'org.newsclub.net.unix.AFUNIXSocketAddress))

(def ^Class junixsocket-server-class
  (find-class 'org.newsclub.net.unix.AFUNIXServerSocket))

(def ^Class jdk-unix-address-class
  (find-class 'java.net.UnixDomainSocketAddress))

(def ^Class jdk-unix-server-class
  (find-class 'java.nio.channels.ServerSocketChannel))

(def ^:private test-junixsocket?
  ;; Make it possible to test junixsocket even when JDK >= 16
  (= "true" (System/getProperty "nrepl.test.junixsocket")))

(def unix-domain-flavor
  (cond
    test-junixsocket? (do
                        (assert junixsocket-address-class)
                        (assert junixsocket-server-class)
                        (binding [*out* *err*]
                          (println "nrepl.test: insisting on junixsocket support"))
                        :junixsocket)
    (and jdk-unix-address-class jdk-unix-server-class) :jdk
    (and junixsocket-address-class junixsocket-server-class) :junixsocket
    :else nil))

(def jdk-unix-address-of
  (when (= :jdk unix-domain-flavor)
    (let [addr-of (.getDeclaredMethod jdk-unix-address-class "of"
                                      (into-array Class [String]))]
      (fn [path] (.invoke addr-of nil (into-array String [path]))))))

(def junix-address-of
  (when (= :junixsocket unix-domain-flavor)
    (let [c (.getDeclaredConstructor junixsocket-address-class
                                     (into-array Class [File]))]
      (fn [path] (.newInstance c (into-array File [(File. ^String path)]))))))

(defn unix-socket-address
  "Returns a filesystem socket address for the given path string."
  [^String path]
  (case unix-domain-flavor
    :jdk (jdk-unix-address-of path)
    :junixsocket (junix-address-of path)
    (let [msg "Support for filesystem sockets requires JDK 16+ or a junixsocket dependency"]
      (log msg)
      (throw (ex-info msg {:nrepl/kind ::no-filesystem-sockets})))))

(set! *warn-on-reflection* false)

;; SocketAddress doesn't have .getPath until JDK 16, and we can't refer to
;; AFUnixSocketAddress unconditionally in the junixsocket cases.  Also note that
;; the former returns a Path, and the latter returns a string.

(defn- get-path [addr] (.getPath addr))

(set! *warn-on-reflection* orig-warn-on-reflection)

(def jdk-unix-server-socket
  ;; Dynamic because one argument open doesn't exist until jvm 15, nor UNIX
  ;; until jvm 16.
  (when (= :jdk unix-domain-flavor)
    (let [protocol (-> (.getDeclaredField StandardProtocolFamily "UNIX")
                       (.get StandardProtocolFamily))
          protocol (into-array ProtocolFamily [protocol])
          open (.getDeclaredMethod ServerSocketChannel "open"
                                   (into-array Class [ProtocolFamily]))]
      #(.invoke open nil protocol))))

(def junix-server-socket
  (when (= :junixsocket unix-domain-flavor)
    (let [make (.getDeclaredMethod junixsocket-server-class "newInstance" nil)]
      #(.invoke make nil nil))))

(defn unix-server-socket
  "Returns a filesystem socket bound to the path if the JDK is version
  16 or newer or if com.kohlschutter.junixsocket/junixsocket-core can
  be loaded dynamically.  Otherwise throws the ex-info map
  {:nrepl/kind ::no-filesystem-sockets}."
  [^String path]
  (let [^SocketAddress addr (unix-socket-address path)]
    (case unix-domain-flavor
      :jdk
      (let [sock (jdk-unix-server-socket)]
        (.bind ^ServerSocketChannel sock addr)
        (let [^Path path (get-path addr)]
          (-> path .toFile .deleteOnExit))
        sock)

      :junixsocket
      (let [sock (junix-server-socket)]
        (.bind ^ServerSocket sock addr)
        (let [^String path (get-path addr)]
          (-> path File. .deleteOnExit))
        sock)

      (let [msg "Support for filesystem sockets requires JDK 16+ or a junixsocket dependency"]
        (log msg)
        (throw (ex-info msg {:nrepl/kind ::no-filesystem-sockets}))))))

(defn as-nrepl-uri [sock transport-scheme]
  (let [get-local-addr (fn [^NetworkChannel c] (.getLocalAddress c))]
    (if-let [addr (and (some-> jdk-unix-server-class (instance? sock))
                       (get-local-addr sock))]
      (URI. (str transport-scheme "+unix")
            (let [^Path path (get-path addr)]
              (-> path .toAbsolutePath str))
            nil)
      (if-let [addr (and (some-> junixsocket-server-class (instance? sock))
                         (.getLocalSocketAddress ^ServerSocket sock))]
        (URI. (str transport-scheme "+unix")
              (get-path addr)
              nil)
        ;; Assume it's an InetAddress socket
        (let [sock ^ServerSocket sock]
          (URI. transport-scheme
                nil ;; userInfo
                (-> sock .getInetAddress .getHostName)
                (.getLocalPort sock)
                nil       ;; path
                nil       ;; query
                nil)))))) ;; fragment

(defprotocol Acceptable
  (accept [s]
    "Accepts a connection on s.  Throws ClosedChannelException if s is
    closed."))

(extend-protocol Acceptable
  ServerSocketChannel
  (accept [s] (.accept s))

  ServerSocket
  (accept [s]
    (when (.isClosed s)
      (throw (ClosedChannelException.)))
    (.accept s)))

;; We have to handle this ourselves for NIO because unfortunately read and write
;; hang if we use both Channels/newInputStream and Channels/newOutputStream.
;; Read and write deadlock on a shared channel input/output stream lock
;; (cf. https://bugs.openjdk.java.net/browse/JDK-4509080).  Verified that this
;; still happens (via thread dump when hung) with jdk 16.

(definterface Writable
  (write [byte-array]
         "Writes the given bytes to the output as per OutputStream write.")
  (write [byte-array offset length]
         "Writes the given bytes to the output as per OutputStream write."))

(defrecord BufferedOutputChannel
           [^SocketChannel channel ^ByteBuffer buffer]

  java.io.Flushable
  (flush [this]
    (.flip buffer)
    (.write channel buffer)
    (.clear buffer))

  Writable
  (write [this byte-array]
    (.write this byte-array 0 (count byte-array)))
  (write [this byte-array offset length]
    (if (> length (.capacity buffer))
      (do
        (.flush this)
        (.write channel (ByteBuffer/wrap byte-array) offset length))
      (do
        (when (> length (.remaining buffer))
          (.flush this))
        (.put buffer byte-array offset length)))))

(defn buffered-output-channel [^SocketChannel channel bytes]
  (assert (.isBlocking channel))
  (->BufferedOutputChannel channel (ByteBuffer/allocate bytes)))

(defprotocol AsBufferedInputStreamSubset
  (buffered-input [x]
    "Returns a buffered stream (subset of BufferedInputStream) reading from x."))

(extend-protocol AsBufferedInputStreamSubset
  ;; Use the Channels stream for input but not output to avoid the deadlock
  SocketChannel (buffered-input [s] (-> s Channels/newInputStream io/input-stream))
  Socket (buffered-input [s] (io/input-stream s))
  BufferedInputStream (buffered-input [s] s))

(defprotocol AsBufferedOutputStreamSubset
  (buffered-output [x]
    "Returns a buffered stream (subset of BufferedOutputStream) reading from x."))

(extend-protocol AsBufferedOutputStreamSubset
  ;; Use the Channels stream for input but not output to avoid the deadlock
  SocketChannel (buffered-output [s] (buffered-output-channel s 8192))
  Socket (buffered-output [s] (io/output-stream s))
  BufferedOutputStream (buffered-output [s] s))
