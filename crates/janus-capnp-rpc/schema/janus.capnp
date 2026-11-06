@0xa1836cd8f010c4b7;

struct Command {
  union {
    set @0 :Set;
    get @1 :Data;
    delete @2 :Data;
    expire @3 :Expire;
    ttl @4 :Data;
    save @5 :Void;
    multi @6 :Void;
    exec @7 :Void;
    discard @8 :Void;
    info @9 :Void;
    traces @10 :Void;
    auth @11 :Data;
  }
}

struct Set {
  key @0 :Data;
  value @1 :Data;
}

struct Expire {
  key @0 :Data;
  seconds @1 :UInt64;
}

struct Response {
  union {
    empty @0 :Void;
    value @1 :Value;
    deleted @2 :Bool;
    integer @3 :Int64;
    queued @4 :Void;
    array @5 :List(Response);
    error @6 :Text;
  }
}

struct Value {
  present @0 :Bool;
  data @1 :Data;
}

interface Janus {
  execute @0 (command :Command) -> (response :Response);
}
