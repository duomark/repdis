%%%------------------------------------------------------------------------------
%%% @copyright (c) 2014, DuoMark International, Inc.
%%% @author Jay Nelson <jay@duomark.com> [http://duomark.com/]
%%% @reference 2014 Development sponsored by TigerText, Inc. [http://tigertext.com/]
%%% @reference The license is based on the template for Modified BSD from
%%%   <a href="http://opensource.org/licenses/BSD-3-Clause">OSI</a>
%%% @doc
%%%   Redis hash functions implemented with process dictionary as the store.
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
         hgetall/1, hgetall/2,
         hget/2, hmget/2, hget/3, hmget/3,
         hset/3, hmset/2, hset/4, hmset/3,
         hdel/2, hdel/3,
         hexists/2, hexists/3,
         hkeys/1, hkeys/2, hlen/1, hlen/2,
         hsetnx/3, hsetnx/4, hvals/1, hvals/2,
         hincrby/3, hincrby/4, hincrbyfloat/3, hincrbyfloat/4
        ]).

-define(LOW_DB,   0).
-define(HIGH_DB, 15).
-define(VALID_DB_NUM(__Num), is_integer(__Num) andalso __Num >= ?LOW_DB andalso __Num =< ?HIGH_DB).

%% External functions for testing
-export([sentinel/0]).

-type db_num()    :: ?LOW_DB .. ?HIGH_DB.
-type key()       :: iodata().
-type field()     :: iodata().
-type set_value() :: iodata().
-type get_value() :: binary() | nil.

-type field_value_pair()  :: {field(), set_value()}.
-type field_value_pairs() :: [field_value_pair()].

-export_type([key/0, field/0, set_value/0, get_value/0, field_value_pairs/0]).


%%%------------------------------------------------------------------------------
%%% External API
%%%------------------------------------------------------------------------------

-spec redis_get(key())                  -> get_value().
-spec redis_get(db_num(), key())        -> get_value().
-spec set(key(), set_value())           -> ok.
-spec set(db_num(), key(), set_value()) -> ok.
-spec del(key())                        -> non_neg_integer().
-spec del(db_num(), key())              -> non_neg_integer().

redis_get(Key) ->
    redis_get(?LOW_DB, Key).

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

-spec hgetall (key() )           -> [binary() | get_value()].  %% Actually pairs flattened.
-spec hgetall (db_num(), key() ) -> [binary() | get_value()].  %% Actually pairs flattened.

hgetall(Key) ->
    hgetall(?LOW_DB, Key).

hgetall(Db_Num, Key)
  when ?VALID_DB_NUM(Db_Num) ->
    Bin_Key = iolist_to_binary(Key),
    case get_dict(Db_Num, Bin_Key) of
        undefined -> [];
        Dict      -> lists:foldr(fun({Field, Val}, Accum) -> [Field, Val | Accum] end,
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
        undefined -> nil;
        Dict      -> get_field_value(Field, Dict)
    end.

hmget(Db_Num, Key, Fields)
  when ?VALID_DB_NUM(Db_Num) ->
    Bin_Key = iolist_to_binary(Key),
    case get_dict(Db_Num, Bin_Key) of
        undefined -> lists:duplicate(length(Fields), nil);
        Dict      -> [get_field_value(Field, Dict) || Field <- Fields]
    end.


-spec hset  (key(), field(), set_value() ) -> 0 | 1.
-spec hmset (key(), field_value_pairs()  ) -> ok.

hset (Key, Field, Value)      -> hset (?LOW_DB, Key, Field, Value).
hmset(Key, Field_Value_Pairs) -> hmset(?LOW_DB, Key, Field_Value_Pairs).


-spec hset  (db_num(), key(), field(), set_value() ) -> 0 | 1.
-spec hmset (db_num(), key(), field_value_pairs()  ) -> binary().

hset(Db_Num, Key, Field, Value)
  when ?VALID_DB_NUM(Db_Num) ->
    Bin_Key = iolist_to_binary(Key),
    case get_dict(Db_Num, Bin_Key) of
        undefined -> set_value_in_new_dict     (Db_Num, Bin_Key, Field, Value);
        Dict      -> set_value_in_existing_dict(Db_Num, Bin_Key, Field, Value, Dict)
    end.

hmset(Db_Num, Key, Field_Value_Pairs)
  when ?VALID_DB_NUM(Db_Num) ->
    Bin_Key = iolist_to_binary(Key),
    case get_dict(Db_Num, Bin_Key) of
        undefined -> set_values_in_new_dict     (Db_Num, Bin_Key, Field_Value_Pairs),       <<"OK">>;
        Dict      -> set_values_in_existing_dict(Db_Num, Bin_Key, Field_Value_Pairs, Dict), <<"OK">>
    end.


-spec hdel(key(), [field()])           -> non_neg_integer().
-spec hdel(db_num(), key(), [field()]) -> non_neg_integer().
    
hdel(Key, Fields) ->
    hdel(?LOW_DB, Key, Fields).

hdel(Db_Num, Key, Fields)
  when ?VALID_DB_NUM(Db_Num) ->
    Bin_Key = iolist_to_binary(Key),
    case get_dict(Db_Num, Bin_Key) of
        undefined -> 0;
        Dict      -> Fold_Fn = fun fold_delete_field_value/2,
                     {Num_Deleted, New_Dict} = lists:foldl(Fold_Fn, {0, Dict}, Fields),
                     Dict = put_dict(Db_Num, Bin_Key, New_Dict),
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
        undefined -> false;
        Dict      -> get_field_value(Field, Dict) =/= nil
    end.


-spec hkeys (key())           -> [key()].
-spec hkeys (db_num(), key()) -> [key()].
-spec hlen  (key())           -> non_neg_integer().
-spec hlen  (db_num(), key()) -> non_neg_integer().

hkeys(Key) ->
    hkeys(?LOW_DB, Key).

hkeys(Db_Num, Key)
  when ?VALID_DB_NUM(Db_Num) ->
    Bin_Key = iolist_to_binary(Key),
    case get_dict(Db_Num, Bin_Key) of
        undefined -> [];
        Dict      -> dict:fetch_keys(Dict)
    end.

hlen(Key) ->
    hlen(?LOW_DB, Key).

hlen(Db_Num, Key)
  when ?VALID_DB_NUM(Db_Num) ->
    Bin_Key = iolist_to_binary(Key),
    case get_dict(Db_Num, Bin_Key) of
        undefined -> 0;
        Dict      -> dict:size(Dict)
    end.


-spec hsetnx(key(), field(), set_value())           -> 0 | 1.
-spec hsetnx(db_num(), key(), field(), set_value()) -> 0 | 1.

hsetnx(Key, Field, Value) ->
    hsetnx(?LOW_DB, Key, Field, Value).

hsetnx(Db_Num, Key, Field, Value)
  when ?VALID_DB_NUM(Db_Num) ->
    Bin_Key = iolist_to_binary(Key),
    case get_dict(Db_Num, Bin_Key) of
        undefined -> set_value_in_new_dict(Db_Num, Bin_Key, Field, Value);
        Dict      -> case get_field_value(Field, Dict) of
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
        undefined -> [];
        Dict      -> [Value || {_Key, Value} <- dict:to_list(Dict)]
    end.


-spec hincrby(key(), field(), integer()) -> integer().
-spec hincrby(db_num(), key(), field(), integer()) -> integer().
-spec hincrbyfloat(key(), field(), float()) -> float().
-spec hincrbyfloat(db_num(), key(), field(), float()) -> float().

hincrby(Key, Field, Increment)
  when is_integer(Increment) ->
    hincrby(?LOW_DB, Key, Field, Increment).

hincrby(Db_Num, Key, Field, Increment)
  when ?VALID_DB_NUM(Db_Num), is_integer(Increment) ->
    Bin_Key = iolist_to_binary(Key),
    case get_dict(Db_Num, Bin_Key) of
        undefined -> set_value_in_new_dict(Db_Num, Bin_Key, Field, Increment),
                     Increment;
        Dict      -> case get_field_value(Field, Dict) of
                         nil   -> set_value_in_existing_dict(Db_Num, Bin_Key, Field, Increment, Dict);
                         Value -> New_Value = integer_to_binary(binary_to_integer(Value) + Increment),
                                  set_value_in_existing_dict(Db_Num, Bin_Key, Field, New_Value, Dict),
                                  New_Value
                     end
    end.

hincrbyfloat(Key, Field, Increment)
  when is_float(Increment) ->
    hincrbyfloat(?LOW_DB, Key, Field, Increment).

hincrbyfloat(Db_Num, Key, Field, Increment)
  when ?VALID_DB_NUM(Db_Num), is_float(Increment) ->
    Bin_Key = iolist_to_binary(Key),
    case get_dict(Db_Num, Bin_Key) of
        undefined -> set_value_in_new_dict(Db_Num, Bin_Key, Field, Increment),
                     Increment;
        Dict      -> case get_field_value(Field, Dict) of
                         nil   -> set_value_in_existing_dict(Db_Num, Bin_Key, Field, Increment, Dict);
                         Value -> New_Value = float_to_binary(binary_to_float(Value) + Increment),
                                  set_value_in_existing_dict(Db_Num, Bin_Key, Field, New_Value, Dict),
                                  New_Value
                     end
    end.


%%%------------------------------------------------------------------------------
%%% Internal utilities
%%%------------------------------------------------------------------------------

sentinel() -> <<"$$REPDIS">>.

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
    
get_dict(Db_Num, Bin_Key)       -> get(make_pd_key(Db_Num, Bin_Key)).
put_dict(Db_Num, Bin_Key, Dict) -> put(make_pd_key(Db_Num, Bin_Key), Dict).

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
    undefined = put_dict(Db_Num, Bin_Key, New_Dict),
    1.

set_value_in_existing_dict(Db_Num, Bin_Key, Field, Value, Dict)
  when is_binary(Bin_Key) ->
    Bin_Field = iolist_to_binary(Field),
    Bin_Value = case Value of
                    undefined -> undefined;
                    Value     -> iolist_to_binary(Value)
                end,
    {Result, New_Dict} = set_field_value(Bin_Field, Bin_Value, Dict),
    Dict = put_dict(Db_Num, Bin_Key, New_Dict),
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
    undefined = put_dict(Db_Num, Bin_Key, New_Dict),
    ok.

set_values_in_existing_dict(Db_Num, Bin_Key, Field_Value_Pairs, Dict)
  when is_binary(Bin_Key) ->
    New_Dict = set_fields(Field_Value_Pairs, Dict),
    Dict = put_dict(Db_Num, Bin_Key, New_Dict),
    ok.
    
set_fields(Field_Value_Pairs, Dict) ->
    lists:foldl(fun({Field, Value}, Next_Dict) ->
                        Bin_Field = iolist_to_binary(Field),
                        Bin_Value = case Value of
                                        undefined -> undefined;
                                        Value     -> iolist_to_binary(Value)
                                    end,
                        dict:store(Bin_Field, Bin_Value, Next_Dict) end,
                Dict, Field_Value_Pairs).

fold_delete_field_value(Field, {Num_Deleted, Curr_Dict} = Curr_Result) ->
    Bin_Field = iolist_to_binary(Field),
    case dict:find(Bin_Field, Curr_Dict) of
        error   -> Curr_Result;
        {ok, _} -> {Num_Deleted + 1, dict:erase(Bin_Field, Curr_Dict)}
    end.
