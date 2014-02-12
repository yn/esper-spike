(ns esper-spike.core
  (:require [esper-spike.esper :as e]))

(defn init-result-val [] {:once 0 :count [] :avg []})
(def esper nil)
(defonce result (atom (init-result-val)))

(defn create-once-listener []
  (let [stmt (e/create-statement "select count(*) from YNSpikeEvent having count(*) = 1")
        once-listener (e/create-listener
                       (fn [_] (swap! result update-in [:once] inc)))]
    (e/attach-listener stmt once-listener)))

(defn create-avg-count-listener []
  (let [stmt (e/create-statement "select count(*) as c, avg(spike_int) as a from YNSpikeEvent")
        avg-count-listener (e/create-listener
                            (fn [m]
                              (swap! result update-in [:count] conj (:c m))
                              (swap! result update-in [:avg] conj (:a m))))]
    (.addListener stmt avg-count-listener)))

(defn send-events []
  (.sendEvent (.getEPRuntime @e/esper) {"spike_int" 5} "YNSpikeEvent")
  (.sendEvent (.getEPRuntime @e/esper) {"spike_int" 6} "YNSpikeEvent")
  (.sendEvent (.getEPRuntime @e/esper) {"spike_int" 10} "YNSpikeEvent"))

(defn go []
  (reset! result (init-result-val))
  (e/create-esper)
  (e/create-statement "create schema YNSpikeEvent (spike_int integer)")
  (create-once-listener)
  (create-avg-count-listener)
  (send-events)
  @result)
