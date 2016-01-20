-module(cberl_queue_tests).
-include_lib("eunit/include/eunit.hrl").
-include_lib("couchbase_connection.hrl").

cberl_test_() ->
    [{foreach, fun setup/0, fun clean_up/1,
      [
       fun test_clean/1
       ,fun test_lenqueue/1
       ,fun test_ldequeue/1
       ,fun test_lremove/1
       ,fun test_lenqueue_len/1
       ,fun test_lcut/1
       ,fun test_clean/1
       ,fun test_lmenqueue_len/1
      ]}].


%%%===================================================================
%%% Setup / Teardown
%%%===================================================================

setup() ->
    cberl:start_link(?POOLNAME, 3,
                     ?COUCHBASE_HOST,
                     ?COUCHBASE_USER,
                     ?COUCHBASE_PASSWORD),
    ok.

clean_up(_) ->
    cberl:stop(?POOLNAME).

%%%===================================================================
%%% Tests
%%%===================================================================

test_clean(_) ->
    cberl:remove(?POOLNAME, <<"testkey">>),
    cberl:remove(?POOLNAME, <<"testkey1">>),
    cberl:remove(?POOLNAME, <<"testkey2">>),
    [].

test_lenqueue(_) ->
    Key = <<"testkey">>,
    Key2 = <<"testkey2">>,
    Value = 1,
    ok = cberl:lenqueue(?POOLNAME, Key, 0, Value),
    Get1 = cberl:lget(?POOLNAME, Key),
    ok = cberl:lenqueue(?POOLNAME, Key, 0, Value),
    Get2 = cberl:lget(?POOLNAME, Key),
    Value2 = 2,
    ok = cberl:lenqueue(?POOLNAME, Key, 0, Value2),
    Get3 = cberl:lget(?POOLNAME, Key),
    GetFail = cberl:lget(?POOLNAME, Key2),
    [?_assertMatch({Key, _, [Value]}, Get1)
     ,?_assertMatch({Key, _, [Value, Value]}, Get2)
     ,?_assertMatch({Key, _, [Value, Value, Value2]}, Get3)
     ,?_assertMatch({Key2, {error, key_enoent}}, GetFail)
    ].

test_ldequeue(_) ->
    Key = <<"testkey">>,
    Value = 1,
    Key2 = <<"testkey2">>,
    Value2 = 2,
    DequeueValue = cberl:ldequeue(?POOLNAME, Key, 0),
    DequeueFail = cberl:ldequeue(?POOLNAME, Key2, 0),
    Get1 = cberl:lget(?POOLNAME, Key),
    [
     ?_assertMatch({Key, _, 1}, DequeueValue)
     ,?_assertMatch({Key, _, [Value, Value2]}, Get1)
     ,?_assertEqual({Key2, {error, key_enoent}}, DequeueFail)
    ].

test_lremove(_) ->
    Key = <<"testkey">>,
    Key2 = <<"testkey2">>,
    Value = 1,
    Value2 = 2,
    ok = cberl:lenqueue(?POOLNAME, Key, 0, Value2),
    ok = cberl:lenqueue(?POOLNAME, Key, 0, Value2),
    ok = cberl:lremove(?POOLNAME, Key, 0, Value2),
    Get = cberl:lget(?POOLNAME, Key),
    ok = cberl:lremove(?POOLNAME, Key, 0, Value2),
    Get1 = cberl:lget(?POOLNAME, Key),
    ok = cberl:lremove(?POOLNAME, Key, 0, Value),
    Get2 = cberl:lget(?POOLNAME, Key),
    RemoveFail = cberl:lremove(?POOLNAME, Key, 0, Value),
    RemoveFail2 = cberl:lremove(?POOLNAME, Key2, 0, Value),
    [
        ?_assertMatch({Key, _, [Value]}, Get),
        ?_assertMatch({Key, _, [Value]}, Get1),
        ?_assertEqual({error, key_enoent}, RemoveFail),
        ?_assertEqual({error, key_enoent}, RemoveFail2),
        ?_assertEqual({Key, {error, key_enoent}}, Get2)
        ].

test_lenqueue_len(_) ->
    Key = <<"testkey">>,
    Key2 = <<"testkey2">>,
    Value = 1,
    ok = cberl:lenqueue_len(?POOLNAME, Key, 0, Value, 1),
    Get1 = cberl:lget(?POOLNAME, Key),
    Len1 = cberl:llen(?POOLNAME, Key),
    ok = cberl:lenqueue_len(?POOLNAME, Key, 0, Value, 1),
    Get2 = cberl:lget(?POOLNAME, Key),
    Len2 = cberl:llen(?POOLNAME, Key),
    Value2 = 2,
    ok = cberl:lenqueue_len(?POOLNAME, Key, 0, Value2, 1),
    Get3 = cberl:lget(?POOLNAME, Key),
    Len3 = cberl:llen(?POOLNAME, Key),
    GetFail = cberl:lget(?POOLNAME, Key2),
    LenFail = cberl:llen(?POOLNAME, Key2),
    [?_assertMatch({Key, _, [Value]}, Get1)
     ,?_assertMatch({Key, _, 1}, Len1)
     ,?_assertMatch({Key, _, [Value, Value]}, Get2)
     ,?_assertMatch({Key, _, 2}, Len2)
     ,?_assertMatch({Key, _, [Value, Value2]}, Get3)
     ,?_assertMatch({Key, _, 2}, Len3)
     ,?_assertMatch({Key2, {error, key_enoent}}, GetFail)
     ,?_assertMatch({Key2, {error, key_enoent}}, LenFail)
    ].

test_lcut(_) ->
    Key = <<"testkey">>,
    Value = 1,
    Value2 = 2,
    Key2 = <<"testkey2">>,
    ok = cberl:lenqueue_len(?POOLNAME, Key, 0, Value, 1),
    ok = cberl:lenqueue_len(?POOLNAME, Key, 0, Value2, 1),
    Get = cberl:lget(?POOLNAME, Key),
    ok = cberl:lcut(?POOLNAME, Key, 0, 50),
    Get1 = cberl:lget(?POOLNAME, Key),
    ok = cberl:lcut(?POOLNAME, Key, 0, 1),
    Get2 = cberl:lget(?POOLNAME, Key),
    ok = cberl:lcut(?POOLNAME, Key, 0, 0),
    GetFail = cberl:lget(?POOLNAME, Key),
    CutFail = cberl:lcut(?POOLNAME, Key, 0, 0),
    CutFail2 = cberl:lcut(?POOLNAME, Key2, 0, 0),
    [
     ?_assertMatch({Key, _, [Value, Value2]}, Get)
     ,?_assertMatch({Key, _, [Value, Value2]}, Get1)
     ,?_assertMatch({Key, _, [Value2]}, Get2)
     ,?_assertMatch({Key, {error, key_enoent}}, GetFail)
     ,?_assertEqual({error, key_enoent}, CutFail)
     ,?_assertEqual({error, key_enoent}, CutFail2)
    ].

test_lmenqueue_len(_) ->
    Key = <<"testkey">>,
    Key2 = <<"testkey2">>,
    Value = 1,
    Value2 = 2,
    LME1 = cberl:lmenqueue_len(?POOLNAME, [{Key, Value, 1}, {Key, Value, 2}, {Key, Value, 2}, {Key, Value2, 2}], 0),
    LML1 = cberl:lmlen(?POOLNAME, [Key, Key2]),
    LMG1 = cberl:lmget(?POOLNAME, [Key, Key2]),
    LMD2 = cberl:lmdequeue(?POOLNAME, [Key, Key2], 0),
    LML2 = cberl:lmlen(?POOLNAME, [Key, Key2]),
    LMG2 = cberl:lmget(?POOLNAME, [Key, Key2]),
    LMC3 = cberl:lmcut(?POOLNAME, [{Key, 50}, {Key2, 50}], 0),
    LMG3 = cberl:lmget(?POOLNAME, [Key, Key2]),
    LMC4 = cberl:lmcut(?POOLNAME, [{Key, 1}, {Key2, 1}], 0),
    LMG4 = cberl:lmget(?POOLNAME, [Key, Key2]),
    LMC5 = cberl:lmcut(?POOLNAME, [{Key, 0}, {Key2, 0}], 0),
    LMG5 = cberl:lmget(?POOLNAME, [Key, Key2]),
    [?_assertMatch({ok, [{Key, ok}, {Key, ok}, {Key, ok}, {Key, ok}]}, LME1)
     ,?_assertMatch([{Key, _, 3}, {Key2, {error, key_enoent}}], LML1)
     ,?_assertMatch([{Key, _, [Value, Value, Value2]}, {Key2, {error, key_enoent}}], LMG1)
     ,?_assertMatch([{Key, _, Value}, {Key2, {error, key_enoent}}], LMD2)
     ,?_assertMatch([{Key, _, 2}, {Key2, {error, key_enoent}}], LML2)
     ,?_assertMatch([{Key, _, [Value, Value2]}, {Key2, {error, key_enoent}}], LMG2)
     ,?_assertMatch({ok, [{Key, ok}, {Key2, {error, key_enoent}}]}, LMC3)
     ,?_assertMatch([{Key, _, [Value, Value2]}, {Key2, {error, key_enoent}}], LMG3)
     ,?_assertMatch({ok, [{Key, ok}, {Key2, {error, key_enoent}}]}, LMC4)
     ,?_assertMatch([{Key, _, [Value2]}, {Key2, {error, key_enoent}}], LMG4)
     ,?_assertMatch({ok, [{Key, ok}, {Key2, {error, key_enoent}}]}, LMC5)
     ,?_assertMatch([{Key, {error, key_enoent}}, {Key2, {error, key_enoent}}], LMG5)
    ].
