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
         hget/2, hmget/2, hget/3, hmget/3,
         hset/3, hmset/2, hset/4, hmset/3,
         hdel/2, hdel/3
        ]).

%% External functions for testing
-export([sentinel/0]).

-type db_num()    :: 0..15.
-type key()       :: iodata().
-type field()     :: iodata().
-type set_value() :: binary().
-type get_value() :: binary() | nil.

-type field_value_pair()  :: {field(), set_value()}.
-type field_value_pairs() :: [field_value_pair()].

-export_type([key/0, field/0, set_value/0, get_value/0, field_value_pairs/0]).


%%%------------------------------------------------------------------------------
%%% External API
%%%------------------------------------------------------------------------------

-spec hget  (key(),  field()  ) ->  get_value().
-spec hmget (key(), [field()] ) -> [get_value()].

hget (Key, Field)  -> hget (0, Key, Field).
hmget(Key, Fields) -> hmget(0, Key, Fields).


-spec hget  (db_num(), key(),  field()  ) ->  get_value().
-spec hmget (db_num(), key(), [field()] ) -> [get_value()].
     
hget(Db_Num, Key, Field) ->
    case get_dict(Db_Num, Key) of
        undefined -> nil;
        Dict      -> get_field_value(Field, Dict)
    end.

hmget(Db_Num, Key, Fields) ->
    case get(make_pd_key(Db_Num, Key)) of
        undefined -> lists:duplicate(length(Fields), nil);
        Dict      -> [get_field_value(Field, Dict) || Field <- Fields]
    end.


-spec hset  (key(), field(), set_value() ) -> 0 | 1.
-spec hmset (key(), field_value_pairs()  ) -> ok.

hset (Key, Field, Value)      -> hset (0, Key, Field, Value).
hmset(Key, Field_Value_Pairs) -> hmset(0, Key, Field_Value_Pairs).


-spec hset  (db_num(), key(), field(), set_value() ) -> 0 | 1.
-spec hmset (db_num(), key(), field_value_pairs()  ) -> ok.

hset(Db_Num, Key, Field, Value) ->
    case get_dict(Db_Num, Key) of
        undefined -> set_value_in_new_dict     (Db_Num, Key, Field, Value);
        Dict      -> set_value_in_existing_dict(Db_Num, Key, Field, Value, Dict)
    end.

hmset(Db_Num, Key, Field_Value_Pairs) ->
    case get_dict(Db_Num, Key) of
        undefined -> set_values_in_new_dict     (Db_Num, Key, Field_Value_Pairs);
        Dict      -> set_values_in_existing_dict(Db_Num, Key, Field_Value_Pairs, Dict)
    end.


-spec hdel(key(), [field()])           -> non_neg_integer().
-spec hdel(db_num(), key(), [field()]) -> non_neg_integer().
    
hdel(Key, Fields) -> hdel(0, Key, Fields).

hdel(Db_Num, Key, Fields) ->
    case get_dict(Db_Num, Key) of
        undefined -> 0;
        Dict      -> Fold_Fn = fun fold_delete_field_value/2,
                     {Num_Deleted, New_Dict} = lists:foldl(Fold_Fn, {0, Dict}, Fields),
                     Dict = put_dict(Db_Num, Key, New_Dict),
                     Num_Deleted
    end.


%%%------------------------------------------------------------------------------
%%% Internal utilities
%%%------------------------------------------------------------------------------

sentinel() -> <<"$$REPDIS">>.

get_dict(Db_Num, Key)       -> get(make_pd_key(Db_Num, Key)).
put_dict(Db_Num, Key, Dict) -> put(make_pd_key(Db_Num, Key), Dict).

make_pd_key(Db_Num, Key) -> {sentinel(), Db_Num, Key}.

get_field_value(Field, Dict) ->
    case dict:find(Field, Dict) of
        error       -> nil;
        {ok, Value} -> Value
    end.
             
set_value_in_new_dict(Db_Num, Key, Field, Value) ->
    New_Dict = dict:store(Field, Value, dict:new()),
    undefined = put_dict(Db_Num, Key, New_Dict),
    1.

set_value_in_existing_dict(Db_Num, Key, Field, Value, Dict) ->
    {Result, New_Dict} = set_field_value(Field, Value, Dict),
    Dict = put_dict(Db_Num, Key, New_Dict),
    Result.

set_field_value(Field, Value, Dict) ->
    case {dict:find(Field, Dict), dict:store(Field, Value, Dict)} of
        {error,   Dict2} -> {1, Dict2};
        {{ok, _}, Dict2} -> {0, Dict2}
    end.

set_values_in_new_dict(Db_Num, Key, Field_Value_Pairs) ->
    New_Dict = set_fields(Field_Value_Pairs, dict:new()),
    undefined = put_dict(Db_Num, Key, New_Dict),
    ok.

set_values_in_existing_dict(Db_Num, Key, Field_Value_Pairs, Dict) ->
    New_Dict = set_fields(Field_Value_Pairs, Dict),
    Dict = put_dict(Db_Num, Key, New_Dict),
    ok.
    
set_fields(Field_Value_Pairs, Dict) ->
    lists:foldl(fun({Field, Value}, Next_Dict) -> dict:store(Field, Value, Next_Dict) end,
                Dict, Field_Value_Pairs).

fold_delete_field_value(Field, {Num_Deleted, Curr_Dict} = Curr_Result) ->
    case dict:find(Field, Curr_Dict) of
        error   -> Curr_Result;
        {ok, _} -> {Num_Deleted + 1, dict:erase(Field, Curr_Dict)}
    end.
