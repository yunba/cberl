-module(cberl_sets_tests).
-include_lib("eunit/include/eunit.hrl").
-include_lib("couchbase_connection.hrl").

cberl_test_() ->
    [{foreach, fun setup/0, fun clean_up/1,
      [
       fun test_sadd/1
       ,fun test_sismember/1
       ,fun test_sremove/1
      ]}].


%%%===================================================================
%%% Setup / Teardown
%%%===================================================================

setup() ->
    cberl:start_link(?POOLNAME, 3,
                     ?COUCHBASE_HOST,
                     ?COUCHBASE_USER,
                     ?COUCHBASE_PASSWORD),
   %cberl:remove(?POOLNAME, <<"testkey">>),
   %cberl:remove(?POOLNAME, <<"testkey1">>),
   %cberl:remove(?POOLNAME, <<"testkey2">>),
    ok.

clean_up(_) ->
    cberl:stop(?POOLNAME).

%%%===================================================================
%%% Tests
%%%===================================================================

test_sadd(_) ->
    Key = <<"testkey">>,
    Key2 = <<"testkey2">>,
    Value = 1,
    ok = cberl:sadd(?POOLNAME, Key, 0, Value),
    Get1 = cberl:sget(?POOLNAME, Key),
    ok = cberl:sadd(?POOLNAME, Key, 0, Value),
    Get2 = cberl:sget(?POOLNAME, Key),
    Value2 = 2,
    ok = cberl:sadd(?POOLNAME, Key, 0, Value2),
    Get3 = cberl:sget(?POOLNAME, Key),
    GetFail = cberl:sget(?POOLNAME, Key2),
    [?_assertMatch({Key, _, [Value]}, Get1)
     ,?_assertMatch({Key, _, [Value]}, Get2)
     ,?_assertMatch({Key, _, [Value, Value2]}, Get3)
     ,?_assertMatch({Key2, {error, key_enoent}}, GetFail)
    ].

test_sismember(_) ->
    Key = <<"testkey">>,
    Value = 1,
    Key2 = <<"testkey2">>,
    Value0 = 0,
    SismemberValue = cberl:sismember(?POOLNAME, Key, 0, Value),
    SismemberFail = cberl:sismember(?POOLNAME, Key, 0, Value0),
    SismemberFail2 = cberl:sismember(?POOLNAME, Key2, 0, Value),
    [
     ?_assertMatch(ok, SismemberValue)
     ,?_assertEqual({error, key_enoent}, SismemberFail)
     ,?_assertEqual({error, key_enoent}, SismemberFail2)
    ].

test_sremove(_) ->
    Key = <<"testkey">>,
    Key2 = <<"testkey2">>,
    Value = 1,
    Value2 = 2,
    ok = cberl:sremove(?POOLNAME, Key, 0, Value2),
    ok = cberl:sremove(?POOLNAME, Key, 0, Value2),
    Get1 = cberl:sget(?POOLNAME, Key),
    ok = cberl:sremove(?POOLNAME, Key, 0, Value),
    Get2 = cberl:sget(?POOLNAME, Key),
    RemoveFail = cberl:sremove(?POOLNAME, Key, 0, Value),
    RemoveFail2 = cberl:sremove(?POOLNAME, Key2, 0, Value),
    [?_assertMatch({Key, _, [Value]}, Get1),
     ?_assertEqual({Key, {error, key_enoent}}, Get2),
     ?_assertEqual({error, key_enoent}, RemoveFail),
     ?_assertEqual({error, key_enoent}, RemoveFail2)
    ].
