(ns rotary.test.client
  (:use [rotary.client])
  (:use [clojure.test])
  (:import [com.amazonaws.services.dynamodbv2.model ConditionalCheckFailedException]))

(def cred {:access-key (get (System/getenv) "AMAZON_SECRET_ID")
           :secret-key (get (System/getenv) "AMAZON_SECRET_ACCESS_KEY")})

(def table "rotary-dev-test")
(def id "test-id")
(def attr "test-attr")

(ensure-table cred {:name "rotary-dev-test" 
                    :hash-key {:name "test-id" :type :s}
                    :throughput {:write 1 :read 1}})

(defn setup-table
  []
  (batch-write-item cred
    [:delete table {id "1"}]
    [:delete table {id "2"}]
    [:delete table {id "3"}]
    [:delete table {id "4"}])
  
  (batch-write-item cred
    [:put table {id "1" attr "foo"}]
    [:put table {id "2" attr "bar"}]
    [:put table {id "3" attr "baz"}]
    [:put table {id "4" attr "foobar"}]))

(deftest test-batch-simple
  (setup-table)
  (let [result (batch-get-item cred {
                 table {
                   :key-name "test-id"
                   :keys ["1" "2" "3" "4"]
                   :consistent true}})
        consis (batch-get-item cred {
                 table {
                   :key-name "test-id"
                   :consistent true
                   :keys ["1" "2" "3" "4"]}})
        attrs (batch-get-item cred {
                 table {
                   :key-name "test-id"
                   :consistent true
                   :attrs [attr]
                   :keys ["1" "2" "3" "4"]}})
        items (get-in result [:responses table])
        item-1 (get-item cred table {id "1"})
        item-2 (get-item cred table {id "2"})
        item-3 (get-item cred table {id "3"})
        item-4 (get-item cred table {id "4"})]

    (is (= "foo" (item-1 attr)) "batch-write-item :put failed")

    (is (= "foo" (item-1 attr)) "batch-write-item :put failed")
    (is (= "bar" (item-2 attr)) "batch-write-item :put failed")
    (is (= "baz" (item-3 attr)) "batch-write-item :put failed")
    (is (= "foobar" (item-4 attr)) "batch-write-item :put failed")

    (is (= true (some #(= (% attr) "foo") items)))
    (is (= true (some #(= (% attr) "bar") (get-in consis [:responses table]))))
    (is (= true (some #(= (% attr) "baz") (get-in attrs [:responses table]))))
    (is (= true (some #(= (% attr) "foobar") items)))

    (batch-write-item cred
      [:delete table {id "1"}]
      [:delete table {id "2"}]
      [:delete table {id "3"}]
      [:delete table {id "4"}])
    
    (is (= nil (get-item cred table {id "1"})) "batch-write-item :delete failed")
    (is (= nil (get-item cred table {id "2"})) "batch-write-item :delete failed")
    (is (= nil (get-item cred table {id "3"})) "batch-write-item :delete failed")
    (is (= nil (get-item cred table {id "4"})) "batch-write-item :delete failed")))

(deftest conditional-put
  (setup-table)
 
  ;; Should update item 1 to have attr bar
  (put-item cred table {id "1" attr "bar"} :expected {attr "foo"})
  (is (= "bar" ((get-item cred table {id "1"}) attr)))
  
  ;; Should fail to update item 2
  (is (thrown? ConditionalCheckFailedException 
               (put-item cred table {id "2" attr "bar"} :expected {id false})))
  (is (not (= "baz" ((get-item cred table {id "2"}) attr))))  

  ;; Should upate item 9 to have attr baz
  (put-item cred table {id "3" attr "foobaz"} :expected {attr "baz"})
  (is (= "foobaz" ((get-item cred table {id "3"}) attr)))

  ;; Should add item 23
  (put-item cred table {id "23" attr "bar"} :expected {id false})
  (is (not (= nil (get-item cred table {id "23"})))))
