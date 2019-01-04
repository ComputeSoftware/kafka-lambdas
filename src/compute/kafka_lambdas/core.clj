(ns compute.kafka-lambdas.core
  (:import (org.apache.kafka.streams.kstream Initializer Aggregator Merger KeyValueMapper ForeachAction Predicate)
           (java.lang.reflect Method)
           (java.lang Thread$UncaughtExceptionHandler)))

(defn- get-lambda-info
  [class-sym]
  (let [methods (-> (str class-sym)
                    (Class/forName)
                    (.getDeclaredMethods))
        _ (assert (= 1 (count methods)) (str class-sym " must be a Java functional interface containing only 1 method."))
        m ^Method (first methods)
        param-count (.getParameterCount m)
        method-name (.getName m)]
    {:method      method-name
     :param-count param-count}))

(defmacro jlambda
  [c args & body]
  (let [actual-count (count args)
        {expected-count :param-count
         method-name    :method} (get-lambda-info c)]
    ;; throw a nice exception when wrong number of args are passed.
    (when (not= actual-count expected-count)
      (throw (ex-info (format "Wrong number of arguments passed to %s. Expected %s and got %s."
                              c expected-count actual-count)
                      {:class    c
                       :expected expected-count
                       :actual   actual-count
                       :args     args})))
    `(reify ~c
       (~(symbol method-name) [this# ~@args]
         ~@body))))

(defmacro initializer
  [args & body]
  `(jlambda Initializer ~args ~@body))

(defmacro aggregator
  [args & body]
  `(jlambda Aggregator ~args ~@body))

(defmacro merger
  [args & body]
  `(jlambda Merger ~args ~@body))

(defmacro key-value-mapper
  [args & body]
  `(jlambda KeyValueMapper ~args ~@body))

(defmacro foreach-action
  [args & body]
  `(jlambda ForeachAction ~args ~@body))

(defmacro predicate
  [args & body]
  `(jlambda Predicate ~args ~@body))

(defmacro uncaught-exception-handler
  [args & body]
  `(jlambda Thread$UncaughtExceptionHandler ~args ~@body))

(defmacro runnable
  [args & body]
  `(jlambda Runnable ~args ~@body))