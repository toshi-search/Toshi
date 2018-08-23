@0xe8842e5d83de58d6;

enum Action {
    create @0;
    update @1;
    delete @2;
}

struct Transaction {
    timestamp @0 :UInt64;
    opscode @1 :UInt32;
    action @2 :Action;
    document @3 :Data;
}