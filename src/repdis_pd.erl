%%%------------------------------------------------------------------------------
%%% @copyright (c) 2014, DuoMark International, Inc.
%%% @author Jay Nelson <jay@duomark.com> [http://duomark.com/]
%%% @reference 2014 Development sponsored by TigerText, Inc. [http://tigertext.com/]
%%% @reference The license is based on the template for Modified BSD from
%%%   <a href="http://opensource.org/licenses/BSD-3-Clause">OSI</a>
%%% @doc
%%%   Redis key, hash and set functions implemented with process dictionary
%%%   as the store. This module is intended for use as a meck implementation
%%%   of redis when testing an application. It removes the need to have a
%%%   redis server running and socket-accessible during the tests.
%%% @since 0.1.0
%%% @end
%%%------------------------------------------------------------------------------
-module(repdis_pd).
-author('Jay Nelson <jay@duomark.com>').

%% External interface
-export([
         %% get is a reserved function, so we'll use redis_get in our API.
         redis_get/1, redis_get/2,
         set/2, set/3,
         del/1, del/2,

         %% Hash API
         hgetall/1, hgetall/2,
         hget/2, hmget/2, hget/3, hmget/3,
         hset/3, hmset/2, hset/4, hmset/3,
         hdel/2, hdel/3,
         hexists/2, hexists/3,
         hkeys/1, hkeys/2, hlen/1, hlen/2,
         hsetnx/3, hsetnx/4, hvals/1, hvals/2,
         hincrby/3, hincrby/4, hincrbyfloat/3, hincrbyfloat/4,

         %% Set API
         sadd/2, sadd/3,
         scard/1, scard/2,
         sdiff/1, sdiff/2,
         sdiffstore/2, sdiffstore/3,
         sinter/1, sinter/2,
         sinterstore/2, sinterstore/3,

         sismember/2, sismember/3,
         smembers/1, smembers/2,
         smove/3, smove/4,
         spop/1, spop/2,
         srandmember/1, srandmember/2, %% srandmember/3,
         srem/2, srem/3,
         sunion/1, sunion/2,
         sunionstore/2, sunionstore/3
        ]).

%% External functions for testing
-export([
         sentinel/0,
         dbsize/0, dbsize/1,
         get_all_data/0, get_all_data/1
        ]).

-define(LOW_DB,   0).
-define(HIGH_DB, 15).
-define(VALID_DB_NUM(__Num), is_integer(__Num) andalso __Num >= ?LOW_DB andalso __Num =< ?HIGH_DB).

-type db_num()    :: ?LOW_DB .. ?HIGH_DB.
-type key()       :: iodata().
-type field()     :: iodata().
-type set_value() :: iodata().
-type get_value() :: binary() | nil.

-type field_value_list() :: [binary()].   % Really an even number of binaries.

-export_type([key/0, field/0, set_value/0, get_value/0, field_value_list/0]).


%%%------------------------------------------------------------------------------
%%% Key External API
%%%------------------------------------------------------------------------------

-spec redis_get(key())                  -> get_value().
-spec redis_get(db_num(), key())        -> get_value().
-spec set(key(), set_value())           -> ok.
-spec set(db_num(), key(), set_value()) -> ok.
-spec del(key())                        -> non_neg_integer().
-spec del(db_num(), key())              -> non_neg_integer().

redis_get(Key) -> redis_get(?LOW_DB, Key).

redis_get(Db_Num, Key)
  when ?VALID_DB_NUM(Db_Num) ->
    Bin_Key = iolist_to_binary(Key),
    get_key(Db_Num, Bin_Key).

set(Key, Value) ->
    set(?LOW_DB, Key, Value).

set(Db_Num, Key, Value)
  when ?VALID_DB_NUM(Db_Num) ->
    Bin_Key   = iolist_to_binary(Key),
    Bin_Value = iolist_to_binary(Value),
    set_key(Db_Num, Bin_Key, Bin_Value).

del(Key) ->
    del(?LOW_DB, Key).

del(Db_Num, Key)
  when ?VALID_DB_NUM(Db_Num) ->
    Bin_Key = iolist_to_binary(Key),
    case erase_key(Db_Num, Bin_Key) of
        undefined -> 0;
        _Value    -> 1
    end.


%%%------------------------------------------------------------------------------
%%% Hash External API
%%%------------------------------------------------------------------------------

-spec hgetall (key() )           -> [binary() | get_value()].  %% Actually pairs flattened.
-spec hgetall (db_num(), key() ) -> [binary() | get_value()].  %% Actually pairs flattened.

hgetall(Key) ->
    hgetall(?LOW_DB, Key).

hgetall(Db_Num, Key)
  when ?VALID_DB_NUM(Db_Num) ->
    Bin_Key = iolist_to_binary(Key),
    case get_dict(Db_Num, Bin_Key) of
        nil  -> [];
        Dict -> lists:foldr(fun({Field, Val}, Accum) -> [Field, Val | Accum] end,
                            [], dict:to_list(Dict))
    end.

-spec hget  (key(),  field()  ) ->  get_value().
-spec hmget (key(), [field()] ) -> [get_value()].

hget (Key, Field)  -> hget (?LOW_DB, Key, Field).
hmget(Key, Fields) -> hmget(?LOW_DB, Key, Fields).


-spec hget  (db_num(), key(),  field()  ) ->  get_value().
-spec hmget (db_num(), key(), [field()] ) -> [get_value()].
     
hget(Db_Num, Key, Field)
  when ?VALID_DB_NUM(Db_Num) ->
    Bin_Key = iolist_to_binary(Key),
    case get_dict(Db_Num, Bin_Key) of
        nil  -> nil;
        Dict -> get_field_value(Field, Dict)
    end.

hmget(Db_Num, Key, Fields)
  when ?VALID_DB_NUM(Db_Num) ->
    Bin_Key = iolist_to_binary(Key),
    case get_dict(Db_Num, Bin_Key) of
        nil  -> lists:duplicate(length(Fields), nil);
        Dict -> [get_field_value(Field, Dict) || Field <- Fields]
    end.


-spec hset  (key(), field(), set_value() ) -> 0 | 1.
-spec hmset (key(), field_value_list()  ) -> binary().

hset (Key, Field, Value)     -> hset (?LOW_DB, Key, Field, Value).
hmset(Key, Field_Value_List) -> hmset(?LOW_DB, Key, Field_Value_List).


-spec hset  (db_num(), key(), field(), set_value() ) -> 0 | 1.
-spec hmset (db_num(), key(), field_value_list()  ) -> binary().

hset(Db_Num, Key, Field, Value)
  when ?VALID_DB_NUM(Db_Num) ->
    Bin_Key = iolist_to_binary(Key),
    case get_dict(Db_Num, Bin_Key) of
        nil  -> set_value_in_new_dict     (Db_Num, Bin_Key, Field, Value);
        Dict -> set_value_in_existing_dict(Db_Num, Bin_Key, Field, Value, Dict)
    end.

hmset(Db_Num, Key, Field_Value_List)
  when ?VALID_DB_NUM(Db_Num) ->
    Bin_Key = iolist_to_binary(Key),
    case get_dict(Db_Num, Bin_Key) of
        nil  -> set_values_in_new_dict     (Db_Num, Bin_Key, Field_Value_List),       <<"OK">>;
        Dict -> set_values_in_existing_dict(Db_Num, Bin_Key, Field_Value_List, Dict), <<"OK">>
    end.


-spec hdel(key(), [field()])           -> non_neg_integer().
-spec hdel(db_num(), key(), [field()]) -> non_neg_integer().
    
hdel(Key, Fields) ->
    hdel(?LOW_DB, Key, Fields).

hdel(Db_Num, Key, Fields)
  when ?VALID_DB_NUM(Db_Num) ->
    Bin_Key = iolist_to_binary(Key),
    case get_dict(Db_Num, Bin_Key) of
        nil  -> 0;
        Dict -> Fold_Fn = fun fold_delete_field_value/2,
                {Num_Deleted, New_Dict} = lists:foldl(Fold_Fn, {0, Dict}, Fields),
                set_key(Db_Num, Bin_Key, New_Dict),
                Num_Deleted
    end.


-spec hexists(key(), field())           -> boolean().
-spec hexists(db_num(), key(), field()) -> boolean().

hexists(Key, Field) ->
    hexists(?LOW_DB, Key, Field).

hexists(Db_Num, Key, Field)
  when ?VALID_DB_NUM(Db_Num) ->
    Bin_Key = iolist_to_binary(Key),
    case get_dict(Db_Num, Bin_Key) of
        nil  -> false;
        Dict -> get_field_value(Field, Dict) =/= nil
    end.


-spec hkeys (key())           -> [key()].
-spec hlen  (key())           -> non_neg_integer().
-spec hkeys (db_num(), key()) -> [key()].
-spec hlen  (db_num(), key()) -> non_neg_integer().

hkeys(Key) -> hkeys(?LOW_DB, Key).
hlen (Key) -> hlen (?LOW_DB, Key).

hkeys(Db_Num, Key)
  when ?VALID_DB_NUM(Db_Num) ->
    Bin_Key = iolist_to_binary(Key),
    case get_dict(Db_Num, Bin_Key) of
        nil  -> [];
        Dict -> dict:fetch_keys(Dict)
    end.

hlen(Db_Num, Key)
  when ?VALID_DB_NUM(Db_Num) ->
    Bin_Key = iolist_to_binary(Key),
    case get_dict(Db_Num, Bin_Key) of
        nil  -> 0;
        Dict -> dict:size(Dict)
    end.


-spec hsetnx(key(), field(), set_value())           -> 0 | 1.
-spec hsetnx(db_num(), key(), field(), set_value()) -> 0 | 1.

hsetnx(Key, Field, Value) ->
    hsetnx(?LOW_DB, Key, Field, Value).

hsetnx(Db_Num, Key, Field, Value)
  when ?VALID_DB_NUM(Db_Num) ->
    Bin_Key = iolist_to_binary(Key),
    case get_dict(Db_Num, Bin_Key) of
        nil  -> set_value_in_new_dict(Db_Num, Bin_Key, Field, Value);
        Dict -> case get_field_value(Field, Dict) of
                    undefined -> set_value_in_existing_dict(Db_Num, Bin_Key, Field, Value, Dict);
                    _Exists   -> 0
                end
    end.
    

-spec hvals(key())           -> [get_value()].
-spec hvals(db_num(), key()) -> [get_value()].

hvals(Key) ->
    hvals(?LOW_DB, Key).

hvals(Db_Num, Key)
  when ?VALID_DB_NUM(Db_Num) ->
    Bin_Key = iolist_to_binary(Key),
    case get_dict(Db_Num, Bin_Key) of
        nil  -> [];
        Dict -> [Value || {_Key, Value} <- dict:to_list(Dict)]
    end.


-spec hincrby      (key(), field(), integer()) -> binary().
-spec hincrbyfloat (key(), field(), float())   -> binary().
-spec hincrby      (db_num(), key(), field(), integer()) -> binary().
-spec hincrbyfloat (db_num(), key(), field(), float())   -> binary().

hincrby(Key, Field, Increment)
  when is_integer(Increment) ->
    hincrby(?LOW_DB, Key, Field, Increment).

hincrbyfloat(Key, Field, Increment)
  when is_float(Increment) ->
    hincrbyfloat(?LOW_DB, Key, Field, Increment).

hincrby(Db_Num, Key, Field, Increment)
  when ?VALID_DB_NUM(Db_Num), is_integer(Increment) ->
    Bin_Key = iolist_to_binary(Key),
    Bin_Increment = integer_to_binary(Increment),
    case get_dict(Db_Num, Bin_Key) of
        nil  -> set_value_in_new_dict(Db_Num, Bin_Key, Field, Bin_Increment),
                Bin_Increment;
        Dict -> case get_field_value(Field, Dict) of
                    nil   -> set_value_in_existing_dict(Db_Num, Bin_Key, Field, Bin_Increment, Dict),
                             Bin_Increment;
                    Value -> New_Value = integer_to_binary(binary_to_integer(Value) + Increment),
                             set_value_in_existing_dict(Db_Num, Bin_Key, Field, New_Value, Dict),
                             New_Value
                end
    end.

hincrbyfloat(Db_Num, Key, Field, Increment)
  when ?VALID_DB_NUM(Db_Num), is_float(Increment) ->
    Bin_Key = iolist_to_binary(Key),
    Bin_Increment = float_to_binary(Increment),
    case get_dict(Db_Num, Bin_Key) of
        nil  -> set_value_in_new_dict(Db_Num, Bin_Key, Field, Bin_Increment),
                Bin_Increment;
        Dict -> case get_field_value(Field, Dict) of
                    nil   -> set_value_in_existing_dict(Db_Num, Bin_Key, Field, Bin_Increment, Dict),
                             Bin_Increment;
                    Value -> New_Value = float_to_binary(binary_to_float(Value) + Increment),
                             set_value_in_existing_dict(Db_Num, Bin_Key, Field, New_Value, Dict),
                             New_Value
                end
    end.


%%%------------------------------------------------------------------------------
%%% Set External API
%%%------------------------------------------------------------------------------

-spec sadd(key(), binary() | [binary()])           -> non_neg_integer().
-spec srem(key(), binary() | [binary()])           -> non_neg_integer().
-spec sadd(db_num(), key(), binary() | [binary()]) -> non_neg_integer().
-spec srem(db_num(), key(), binary() | [binary()]) -> non_neg_integer().

sadd(Key, Member) -> sadd(?LOW_DB, Key, Member).
srem(Key, Member) -> srem(?LOW_DB, Key, Member).

sadd(Db_Num, Key, Member)
  when ?VALID_DB_NUM(Db_Num), is_binary(Member) ->
    Bin_Key = iolist_to_binary(Key),
    case get_set(Db_Num, Bin_Key) of
        nil -> set_key(Db_Num, Bin_Key, sets:from_list([Member])), 1;
        Set -> New_Set = sets:add_element(Member, Set),
               set_key(Db_Num, Bin_Key, New_Set),
               sets:size(New_Set) - sets:size(Set)
    end;
sadd(Db_Num, Key, [_Member | _More_Members] = Members)
  when ?VALID_DB_NUM(Db_Num), is_binary(_Member), is_list(_More_Members) ->
    Bin_Key    = iolist_to_binary(Key),
    Member_Set = sets:from_list(Members),
    case get_set(Db_Num, Bin_Key) of
        nil -> set_key(Db_Num, Bin_Key, Member_Set), sets:size(Member_Set);
        Set -> New_Set = sets:union(Set, Member_Set),
               set_key(Db_Num, Bin_Key, New_Set),
               sets:size(New_Set) - sets:size(Set)
    end.

srem(Db_Num, Key, Member)
  when ?VALID_DB_NUM(Db_Num), is_binary(Member) ->
    Bin_Key = iolist_to_binary(Key),
    case get_set(Db_Num, Bin_Key) of
        nil -> 0;
        Set -> New_Set = sets:del_element(Member, Set),
               set_key(Db_Num, Bin_Key, New_Set),
               sets:size(Set) - sets:size(New_Set)
    end;
srem(Db_Num, Key, [_Member | _More_Members] = Members)
  when ?VALID_DB_NUM(Db_Num), is_binary(_Member), is_list(_More_Members) ->
    Bin_Key    = iolist_to_binary(Key),
    Member_Set = sets:from_list(Members),
    case get_set(Db_Num, Bin_Key) of
        nil -> 0;
        Set -> New_Set = sets:subtract(Set, Member_Set),
               set_key(Db_Num, Bin_Key, New_Set),
               sets:size(Set) - sets:size(New_Set)
    end.


-spec spop(key())           -> get_value().
-spec spop(db_num(), key()) -> get_value().

spop(Key) -> spop(?LOW_DB, Key).
spop(Db_Num, Key)
  when ?VALID_DB_NUM(Db_Num) ->
    Bin_Key = iolist_to_binary(Key),
    case get_set(Db_Num, Bin_Key) of
        nil -> nil;
        Set -> case sets:to_list(Set) of
                   [] -> nil;
                   Members ->
                       {Popped, Remaining} = select_random(Members),
                       set_key(Db_Num, Key, sets:from_list(Remaining)),
                       Popped
               end
    end.

select_random(Members)
  when is_list(Members) ->
    Size   = length(Members),
    Choice = random:uniform(Size),
    {Skipped, [Chosen | Rest]} = lists:split(Choice-1, Members),
    {Chosen, Skipped ++ Rest}.


-spec srandmember(key()) -> get_value().
-spec srandmember(db_num(), key()) -> get_value().

srandmember(Key) -> srandmember(?LOW_DB, Key).
srandmember(Db_Num, Key)
  when ?VALID_DB_NUM(Db_Num) ->
    Bin_Key = iolist_to_binary(Key),
    case get_set(Db_Num, Bin_Key) of
        nil -> nil;
        Set -> case sets:to_list(Set) of
                   []      -> nil;
                   Members -> {Chosen, _} = select_random(Members),
                              Chosen
               end
    end.


-spec scard(key())           -> non_neg_integer().
-spec scard(db_num(), key()) -> non_neg_integer().

scard(Key) -> scard(?LOW_DB, Key).
scard(Db_Num, Key)
  when ?VALID_DB_NUM(Db_Num) ->
    Bin_Key = iolist_to_binary(Key),
    case get_set(Db_Num, Bin_Key) of
        nil -> 0;
        Set -> sets:size(Set)
    end.


-spec sismember(key(), set_value())           -> 0 | 1.
-spec sismember(db_num(), key(), set_value()) -> 0 | 1.

sismember(Key, Member) -> sismember(?LOW_DB, Key, Member).
sismember(Db_Num, Key, Member)
  when ?VALID_DB_NUM(Db_Num) ->
    Bin_Key = iolist_to_binary(Key),
    case get_set(Db_Num, Bin_Key) of
        nil -> 0;
        Set -> Bin_Member = iolist_to_binary(Member),
               case sets:is_element(Bin_Member, Set) of
                   true  -> 1;
                   false -> 0
               end
    end.


-spec smembers(key())           -> [binary()].
-spec smembers(db_num(), key()) -> [binary()].

smembers(Key) -> smembers(?LOW_DB, Key).
smembers(Db_Num, Key)
  when ?VALID_DB_NUM(Db_Num) ->
    Bin_Key = iolist_to_binary(Key),
    case get_set(Db_Num, Bin_Key) of
        nil -> [];
        Set -> sets:to_list(Set)
    end.


-spec smove(key(), key(), set_value())           -> 0 | 1.
-spec smove(db_num(), key(), key(), set_value()) -> 0 | 1.

smove(Source, Dest, Member) -> smove(?LOW_DB, Source, Dest, Member).
smove(Db_Num, Source, Dest, Member)
  when ?VALID_DB_NUM(Db_Num) ->
    Bin_Dest   = iolist_to_binary(Dest),
    Bin_Source = iolist_to_binary(Source),
    Bin_Member = iolist_to_binary(Member),

    %% Cannot do atomically, never allow disappearance of member during move.
    case {get_set(Db_Num, Bin_Source), get_set(Db_Num, Bin_Dest)} of
        {nil, _} -> 0;
        {Source_Set, Dest_Set} ->
            case sets:is_element(Bin_Member, Source_Set) of
                false -> 0;
                true  -> case Dest_Set of
                             nil      -> set_key(Db_Num, Bin_Dest, sets:from_list([Bin_Member]));
                             Dest_Set -> case sets:is_element(Bin_Member, Dest_Set) of
                                             false -> 0;
                                             true  -> set_key(Db_Num, Bin_Dest,
                                                              sets:add_element(Bin_Member, Dest_Set)),
                                                      set_key(Db_Num, Bin_Source,
                                                              sets:del_element(Bin_Member, Source_Set)),
                                                      1
                                         end
                         end
            end
    end.


-spec sdiff([key()])            -> [key()].
-spec sinter([key()])           -> [key()].
-spec sunion([key()])           -> [key()].

-spec sdiff(db_num(), [key()])  -> [key()].
-spec sinter(db_num(), [key()]) -> [key()].
-spec sunion(db_num(), [key()]) -> [key()].

sdiff (Keys) -> sdiff (?LOW_DB, Keys).
sinter(Keys) -> sinter(?LOW_DB, Keys).
sunion(Keys) -> sinter(?LOW_DB, Keys).

sdiff(Db_Num, Keys) ->
    fold_set(Db_Num, Keys, diff).

sinter(Db_Num, Keys) ->
    fold_set(Db_Num, Keys, inter).

sunion(Db_Num, Keys) ->
    fold_set(Db_Num, Keys, union).
    

-spec sdiffstore (key(), [key()])           -> non_neg_integer().
-spec sinterstore(key(), [key()])           -> non_neg_integer().
-spec sunionstore(key(), [key()])           -> non_neg_integer().

-spec sdiffstore (db_num(), key(), [key()]) -> non_neg_integer().
-spec sinterstore(db_num(), key(), [key()]) -> non_neg_integer().
-spec sunionstore(db_num(), key(), [key()]) -> non_neg_integer().

sdiffstore (Dest_Key, Keys) -> sdiffstore (?LOW_DB, Dest_Key, Keys).
sinterstore(Dest_Key, Keys) -> sinterstore(?LOW_DB, Dest_Key, Keys).
sunionstore(Dest_Key, Keys) -> sinterstore(?LOW_DB, Dest_Key, Keys).

sdiffstore(Db_Num, Dest_Key, Keys)
  when ?VALID_DB_NUM(Db_Num) ->
    Bin_Dest_Key = iolist_to_binary(Dest_Key),
    Remaining_Keys = sdiff(Db_Num, Keys),
    set_key(Db_Num, Bin_Dest_Key, Remaining_Keys),
    length(Remaining_Keys).

sinterstore(Db_Num, Dest_Key, Keys)
  when ?VALID_DB_NUM(Db_Num) ->
    Bin_Dest_Key = iolist_to_binary(Dest_Key),
    Remaining_Keys = sinter(Db_Num, Keys),
    set_key(Db_Num, Bin_Dest_Key, Remaining_Keys),
    length(Remaining_Keys).

sunionstore(Db_Num, Dest_Key, Keys)
  when ?VALID_DB_NUM(Db_Num) ->
    Bin_Dest_Key = iolist_to_binary(Dest_Key),
    Remaining_Keys = sunion(Db_Num, Keys),
    set_key(Db_Num, Bin_Dest_Key, Remaining_Keys),
    length(Remaining_Keys).


%%%------------------------------------------------------------------------------
%%% Database utilities
%%%------------------------------------------------------------------------------

-define(SENTINEL, <<"$$REPDIS">>).

-spec sentinel() -> binary().

sentinel() -> ?SENTINEL.

-spec dbsize()         -> non_neg_integer().
-spec dbsize(db_num()) -> non_neg_integer().

dbsize()       -> dbsize(?LOW_DB).
dbsize(Db_Num) 
  when ?VALID_DB_NUM(Db_Num) ->
    lists:foldl(fun({{?SENTINEL, Db_Id, _Key}, _Value}, Count)
                      when Db_Id =:= Db_Num -> Count+1;
                   (_, Count) -> Count
                end, 0, get()).

-spec get_all_data()         -> [{key(), binary() | dict() | set()}].
-spec get_all_data(db_num()) -> [{key(), binary() | dict() | set()}].
    
get_all_data() -> get_all_data(?LOW_DB).
get_all_data(Db_Num)
  when ?VALID_DB_NUM(Db_Num) ->
    [{Db_Key, Db_Value}
     || {{?SENTINEL, Db_Id, Db_Key}, Db_Value} <- get(),
        Db_Id =:= Db_Num
    ].
    

%%%------------------------------------------------------------------------------
%%% Internal utilities
%%%------------------------------------------------------------------------------

-define(IS_DICT(__Value), is_tuple(__Value) andalso element(1, __Value) =:= dict).
-define(IS_SET (__Value),  sets:is_set(__Value)).

get_key(Db_Num, Bin_Key) ->
    case get(make_pd_key(Db_Num, Bin_Key)) of
        undefined -> nil;
        Bin_Value -> Bin_Value
    end.

set_key(Db_Num, Bin_Key, Bin_Value) ->
    put(make_pd_key(Db_Num, Bin_Key), Bin_Value),
    ok.

erase_key(Db_Num, Bin_Key) ->
    erase(make_pd_key(Db_Num, Bin_Key)).
    
get_set(Db_Num, Bin_Key) ->
    case get(make_pd_key(Db_Num, Bin_Key)) of
        undefined -> nil;
        Value     -> case ?IS_SET(Value) of
                         true  -> Value;
                         false -> throw(not_a_set)
                     end
    end.
    
get_dict(Db_Num, Bin_Key) ->
    case get(make_pd_key(Db_Num, Bin_Key)) of
        undefined -> nil;
        Value when ?IS_DICT(Value) -> Value;
        _   -> throw(not_a_dict)
    end.

make_pd_key(Db_Num, Bin_Key) ->
    {sentinel(), Db_Num, Bin_Key}.

get_field_value(Field, Dict) ->
    Bin_Field = iolist_to_binary(Field),
    case dict:find(Bin_Field, Dict) of
        error       -> nil;
        {ok, Value} -> Value
    end.
             
set_value_in_new_dict(Db_Num, Bin_Key, Field, Value)
  when is_binary(Bin_Key) ->
    Bin_Field = iolist_to_binary(Field),
    Bin_Value = case Value of
                    undefined -> undefined;
                    Value     -> iolist_to_binary(Value)
                end,
    New_Dict = dict:store(Bin_Field, Bin_Value, dict:new()),
    set_key(Db_Num, Bin_Key, New_Dict),
    1.

set_value_in_existing_dict(Db_Num, Bin_Key, Field, Value, Dict)
  when is_binary(Bin_Key) ->
    Bin_Field = iolist_to_binary(Field),
    Bin_Value = case Value of
                    undefined -> undefined;
                    Value     -> iolist_to_binary(Value)
                end,
    {Result, New_Dict} = set_field_value(Bin_Field, Bin_Value, Dict),
    set_key(Db_Num, Bin_Key, New_Dict),
    Result.

set_field_value(Bin_Field, Bin_Value, Dict)
  when is_binary(Bin_Field), is_binary(Bin_Value) orelse Bin_Value =:= undefined ->
    case {dict:find(Bin_Field, Dict), dict:store(Bin_Field, Bin_Value, Dict)} of
        {error,   Dict2} -> {1, Dict2};
        {{ok, _}, Dict2} -> {0, Dict2}
    end.

set_values_in_new_dict(Db_Num, Bin_Key, Field_Value_Pairs)
  when is_binary(Bin_Key) ->
    New_Dict = set_fields(Field_Value_Pairs, dict:new()),
    set_key(Db_Num, Bin_Key, New_Dict),
    ok.

set_values_in_existing_dict(Db_Num, Bin_Key, Field_Value_Pairs, Dict)
  when is_binary(Bin_Key) ->
    New_Dict = set_fields(Field_Value_Pairs, Dict),
    set_key(Db_Num, Bin_Key, New_Dict),
    ok.
    
set_fields([], Dict) -> Dict;
set_fields([Field, Value | More], Dict) ->
    Bin_Field = iolist_to_binary(Field),
    Bin_Value = case Value of
                    undefined -> undefined;
                    Value     -> iolist_to_binary(Value)
                end,
    Next_Dict = dict:store(Bin_Field, Bin_Value, Dict),
    set_fields(More, Next_Dict).

fold_delete_field_value(Field, {Num_Deleted, Curr_Dict} = Curr_Result) ->
    Bin_Field = iolist_to_binary(Field),
    case dict:find(Bin_Field, Curr_Dict) of
        error   -> Curr_Result;
        {ok, _} -> {Num_Deleted + 1, dict:erase(Bin_Field, Curr_Dict)}
    end.

fold_set(Db_Num, [First_Key | Rest_Keys], Fold_Type) ->
    case get_set(Db_Num, iolist_to_binary(First_Key)) of
        nil -> [];
        Start_Set ->
            lists:foldl(fun(Key, Current_Set) ->
                                Bin_Key = iolist_to_binary(Key),
                                case get_set(Db_Num, Bin_Key) of
                                    nil -> Current_Set;
                                    Set -> case Fold_Type of
                                               diff  -> sets:subtract     (Current_Set, Set); 
                                               inter -> sets:intersection (Current_Set, Set);
                                               union -> sets:union        (Current_Set, Set)
                                           end
                                end
                        end, Start_Set, Rest_Keys)
    end.
