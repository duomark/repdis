%%%------------------------------------------------------------------------------
%%% @copyright (c) 2014, DuoMark International, Inc.
%%% @author Jay Nelson <jay@duomark.com> [http://duomark.com/]
%%% @reference 2014 Development sponsored by TigerText, Inc. [http://tigertext.com/]
%%% @reference The license is based on the template for Modified BSD from
%%%   <a href="http://opensource.org/licenses/BSD-3-Clause">OSI</a>
%%% @doc
%%%   Common_test invoking PropEr for repdis process dictionary module.
%%% @since 0.1.0
%%% @end
%%%------------------------------------------------------------------------------
-module(repdis_pd_SUITE).
-auth('jay@duomark.com').
-vsn('').

-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([
         check_keys/1,
         check_hashes/1
        ]).

-include("repdis_common_test.hrl").

all() -> [
          check_keys,
          check_hashes
         ].

init_per_suite(Config) -> Config.
end_per_suite(Config)  -> Config.

%% Test Modules is ?TM
-define(TM, repdis_pd).

get_repdis_values() ->
    [Entry || Entry = {{Sentinel, _Db_Num, _Key}, _Value} <- get(),
              Sentinel =:= ?TM:sentinel()].

-spec check_keys(config()) -> ok.
check_keys(_Config) ->

    ct:log("Fetch 1 field from an empty dictionary always returns nil"),
    [] = get_repdis_values(),
    Test_Empty_Dict_Get
        = ?FORALL({Key}, {?TM:key()},
                  nil =:= ?TM:redis_get(Key)),
    true = proper:quickcheck(Test_Empty_Dict_Get, ?PQ_NUM(10)),
    [] = get_repdis_values(),
    
    ct:log("Get should return any field that is stored by Set"),
    Test_Empty_Dict_Set
        = ?FORALL({Key, Value}, {?TM:key(), ?TM:set_value()},
                  begin
                      Bin_Value = iolist_to_binary(Value),
                      nil       = ?TM:redis_get(Key),     % Currently not present
                      ok        = ?TM:set(Key, Value),    % Store the new value
                      Bin_Value = ?TM:redis_get(Key),     % hget retrieves the value
                      Bin_Value = ?TM:redis_get(Key),     % 2nd hget to see it still there
                      1         = ?TM:del(Key),           % hdel deletes the value
                      nil       = ?TM:redis_get(Key),     % Currently not present
                      true
                  end),
    true = proper:quickcheck(Test_Empty_Dict_Set).
    
-spec check_hashes(config()) -> ok.
check_hashes(_Config) ->

    ct:log("Fetch 1 field from an empty dictionary always returns nil"),
    [] = get_repdis_values(),
    Test_Empty_Dict_Hget
        = ?FORALL({Key, Field}, {?TM:key(), ?TM:field()},
                  nil =:= ?TM:hget(Key, Field)),
    true = proper:quickcheck(Test_Empty_Dict_Hget, ?PQ_NUM(10)),
    [] = get_repdis_values(),

    ct:log("Fetch N fields from an empty dictionary always returns [nil, ...]"),
    Test_Empty_Dict_Hmget
        = ?FORALL({Key, Fields}, {?TM:key(), [?TM:field()]},
                  begin
                      Size = length(Fields),
                      lists:duplicate(Size, nil) =:= ?TM:hmget(Key, Fields)
                  end),
    true = proper:quickcheck(Test_Empty_Dict_Hmget, ?PQ_NUM(10)),
    [] = get_repdis_values(),
    
    ct:log("Hget should return any field that is stored by hset"),
    Test_Empty_Dict_Hset
        = ?FORALL({Key, Field, Value}, {?TM:key(), ?TM:field(), ?TM:set_value()},
                  begin
                      Bin_Value = iolist_to_binary(Value),
                      nil       = ?TM:hget(Key, Field),         % Currently not present
                      1         = ?TM:hset(Key, Field, Value),  % Store the new value
                      Bin_Value = ?TM:hget(Key, Field),         % hget retrieves the value
                      Bin_Value = ?TM:hget(Key, Field),         % 2nd hget to see it still there
                      1         = ?TM:hdel(Key, [Field]),       % hdel deletes the value
                      nil       = ?TM:hget(Key, Field),         % Currently not present
                      true
                  end),
    true = proper:quickcheck(Test_Empty_Dict_Hset),
    
    ct:log("Hmget should return any field that is stored by hmset"),
    Test_Empty_Dict_Hmset
        = ?FORALL({Key, FV_Pairs_With_Dups}, {?TM:key(), ?TM:field_value_pairs()},
                  begin
                      Bin_FV_Pairs_With_Dups = [{iolist_to_binary(F), iolist_to_binary(V)}
                                                || {F, V} <- FV_Pairs_With_Dups],
                      FV_Pairs = dict:to_list(dict:from_list(Bin_FV_Pairs_With_Dups)),
                      {Fields, Values} = lists:unzip(FV_Pairs),
                      Size   = length(Fields),
                      Nils   = lists:duplicate(Size, nil),
                      Nils   = ?TM:hmget(Key, Fields),       % Currently not present
                      ok     = ?TM:hmset(Key, FV_Pairs),     % Store the new values
                      Values = ?TM:hmget(Key, Fields),       % hget retrieves the values
                      Values = ?TM:hmget(Key, Fields),       % hget retrieves the values
                      Size   = ?TM:hdel (Key, Fields),       % hdel deletes the values
                      Nils   = ?TM:hmget(Key, Fields),       % Currently not present
                      true
                  end),
    true = proper:quickcheck(Test_Empty_Dict_Hmset),

    ok.
