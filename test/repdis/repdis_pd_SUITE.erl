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
-author('jay@duomark.com').
-vsn('').

-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([
         check_keys/1,
         check_hashes/1,
         check_sets/1
        ]).

-include("repdis_common_test.hrl").

all() -> [
          check_keys,
          check_hashes,
          check_sets
         ].

init_per_suite(Config) -> Config.
end_per_suite(Config)  -> Config.

%% Test Modules is ?TM
-define(TM, repdis_pd).

-spec check_keys(config()) -> ok.
check_keys(_Config) ->

    ct:log("Fetch 1 field from an empty dictionary always returns nil"),
    [] = ?TM:get_all_data(),
    Test_Empty_Dict_Get
        = ?FORALL({Key}, {?TM:key()},
                  nil =:= ?TM:redis_get(Key)),
    true = proper:quickcheck(Test_Empty_Dict_Get, ?PQ_NUM(10)),
    [] = ?TM:get_all_data(),
    
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
    [] = ?TM:get_all_data(),
    Test_Empty_Dict_Hget
        = ?FORALL({Key, Field}, {?TM:key(), ?TM:field()},
                  nil =:= ?TM:hget(Key, Field)),
    true = proper:quickcheck(Test_Empty_Dict_Hget, ?PQ_NUM(10)),
    [] = ?TM:get_all_data(),

    ct:log("Fetch N fields from an empty dictionary always returns [nil, ...]"),
    Test_Empty_Dict_Hmget
        = ?FORALL({Key, Fields}, {?TM:key(), [?TM:field()]},
                  begin
                      Size = length(Fields),
                      lists:duplicate(Size, nil) =:= ?TM:hmget(Key, Fields)
                  end),
    true = proper:quickcheck(Test_Empty_Dict_Hmget, ?PQ_NUM(10)),
    [] = ?TM:get_all_data(),
    
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
        = ?FORALL({Key, FV_Pairs_With_Dups}, {?TM:key(), ?TM:field_value_list()},
                  begin
                      Bin_FV_Pairs_With_Dups = [{iolist_to_binary(F), iolist_to_binary(V)}
                                                || {F, V} <- FV_Pairs_With_Dups],
                      FV_Pairs   = dict:to_list(dict:from_list(Bin_FV_Pairs_With_Dups)),
                      Flat_Pairs = flatten(FV_Pairs),
                      {Fields, Values} = lists:unzip(FV_Pairs),
                      Size     = length(Fields),
                      Nils     = lists:duplicate(Size, nil),
                      Nils     = ?TM:hmget(Key, Fields),       % Currently not present
                      <<"OK">> = ?TM:hmset(Key, Flat_Pairs),   % Store the new values
                      Values   = ?TM:hmget(Key, Fields),       % hget retrieves the values
                      Values   = ?TM:hmget(Key, Fields),       % hget retrieves the values
                      Size     = ?TM:hdel (Key, Fields),       % hdel deletes the values
                      Nils     = ?TM:hmget(Key, Fields),       % Currently not present
                      true
                  end),
    true = proper:quickcheck(Test_Empty_Dict_Hmset),

    ok.

flatten(KV_Pairs) ->
    lists:foldr(fun({K, V}, Acc) -> [K, V | Acc] end, [], KV_Pairs).

-spec check_sets(config()) -> ok.
check_sets(_Config) ->

    ct:log("Sismember from an empty set always returns 0"),
    [] = ?TM:get_all_data(),
    Test_Empty_Set_Sismember
        = ?FORALL({Key, Member}, {?TM:key(), ?TM:set_value()},
                  begin
                      [] = ?TM:smembers(Key),
                      0  = ?TM:scard(Key),
                      0  = ?TM:sismember(Key, Member),
                      true
                  end),
    true = proper:quickcheck(Test_Empty_Set_Sismember, ?PQ_NUM(10)),
    [] = ?TM:get_all_data(),
    
    ct:log("Sismember should find any member that is stored by sadd"),
    Test_Element_Set_Sismember
        = ?FORALL({Key, Member}, {?TM:key(), binary()},
                  begin
                      Bin_Member = iolist_to_binary(Member),
                      0 = ?TM:sismember (Key, Member),    % Currently not present
                      0 = ?TM:scard     (Key),            % Currently no members
                      [] = ?TM:smembers (Key),            % Currently no members
                      1 = ?TM:sadd      (Key, Member),    % Add member to the set
                      1 = ?TM:sismember (Key, Member),    % Now present
                      1 = ?TM:scard     (Key),            % There is only one member
                      [Bin_Member] = ?TM:smembers(Key),   % This is the only member
                      1 = ?TM:srem      (Key, Member),    % sdel removes the member
                      0 = ?TM:scard     (Key),            % No members left
                      [] = ?TM:smembers (Key),
                      true
                  end),
    true = proper:quickcheck(Test_Element_Set_Sismember, ?PQ_NUM(10)).
