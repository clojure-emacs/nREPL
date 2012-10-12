(ns ^{:author "Chas Emerick"}
     clojure.tools.nrepl.middleware.load-file
  (:require [clojure.tools.nrepl.middleware.interruptible-eval :as eval])
  (:use [clojure.tools.nrepl.middleware :as middleware :only (set-descriptor!)]))

; need to hold file contents "out of band" so as to avoid JVM method
; size limitations (cannot eval an expression larger than some size
; [64k?]), so the naive approach of just interpolating file contents
; into an expression to be evaluated doesn't work
; see http://code.google.com/p/counterclockwise/issues/detail?id=429
; and http://groups.google.com/group/clojure/browse_thread/thread/f54044da06b9939f
(defonce ^{:private true
           :doc "An atom that temporarily holds the contents of files to
be loaded."} file-contents (atom {}))

(defn ^{:dynamic true} load-file-code
  "Given the contents of a file, its _source-path-relative_ path,
   and its filename, returns a string of code (or seq of expressions)
   that, when evaluated, will load those contents with appropriate
   filename references and line numbers in metadata, etc."
  [file file-path file-name]
  ; mini TTL impl so that any code orphaned by errors that occur
  ; between here and the evaluation of the Compiler/load expression
  ; below are cleaned up on subsequent loads  
  (let [t (System/currentTimeMillis)
        file-key ^{:t t} [file-path (gensym)]]
    (swap! file-contents
      (fn [file-contents]
        (let [expired-keys
              (filter
                (comp #(and %
                            (< 10000 (- (System/currentTimeMillis) %)))
                      :t meta)
                (keys file-contents))]
          (assoc (apply dissoc file-contents expired-keys)
                 file-key file))))
    (pr-str `(try
               (clojure.lang.Compiler/load
                 (java.io.StringReader. (@@(var file-contents) '~file-key))
                 ~file-path
                 ~file-name)
               (finally
                 (swap! @(var file-contents) dissoc '~file-key))))))

(defn wrap-load-file
  "Middleware that evaluates a file's contents, as per load-file,
   but with all data supplied in the sent message (i.e. safe for use
   with remote REPL environments).

   This middleware depends on the availability of an :op \"eval\"
   middleware below it (such as interruptable-eval)."
  [h]
  (fn [{:keys [op file file-name file-path] :as msg}]
    (if (not= op "load-file")
      (h msg)
      (h (assoc msg
           :op "eval"
           :code (load-file-code file file-path file-name))))))

(set-descriptor! #'wrap-load-file
  {:requires #{}
   :expects #{"eval"}
   :handles {"load-file"
             {:doc "Loads a body of code, using supplied path and filename info to set source file and line number metadata. Delegates to underlying \"eval\" middleware/handler."
              :requires {"file" "Full contents of a file of code."}
              :optional {"file-path" "Source-path-relative path of the source file, e.g. clojure/java/io.clj"
                         "file-name" "Name of source file, e.g. io.clj"}
              :returns (-> (meta #'eval/interruptible-eval)
                         ::middleware/descriptor
                         :handles
                         (get "eval")
                         :returns)}}})