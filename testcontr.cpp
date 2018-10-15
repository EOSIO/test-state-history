#include <eosiolib/eosio.hpp>
#include <eosiolib/transaction.hpp>

using namespace eosio;

CONTRACT testcontr : public eosio::contract
{
  public:
    using contract::contract;

    ACTION startit()
    {
        print("startit");
        transaction t;
        t.delay_sec = 10;
        t.actions.push_back(
            eosio::action{
                eosio::permission_level{_self, "active"_n},
                _self, "runit"_n, std::tuple<>{}});
        t.send(1000, _self);
    }

    ACTION runit()
    {
        print("runit");
        eosio_assert(false, "fail");
    }

    ACTION onerror()
    {
        print("oops");
        eosio_assert(false, "fail2");
    }
};

extern "C" void apply(uint64_t receiver, uint64_t code, uint64_t action)
{
    switch (action)
    {
        EOSIO_DISPATCH_HELPER(testcontr, (startit)(runit)(onerror))
    }
}
