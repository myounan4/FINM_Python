# main.py
from fix_parser import FixParser
from order import Order, OrderState
from risk_engine import RiskEngine
from logger import Logger


def handle_message(raw, fix: FixParser, risk: RiskEngine, log: Logger):
    msg = fix.parse(raw)

    # FIX tags we care about:
    # 55 = Symbol, 38 = OrderQty, 54 = Side
    symbol = msg["55"]
    qty = int(msg["38"])
    side = msg["54"]

    order = Order(symbol, qty, side)
    log.log("OrderCreated", msg)

    try:
        # Check risk before ACK
        risk.check(order)
        order.transition(OrderState.ACKED)

        # Simulate full fill immediately
        risk.update_position(order)
        order.transition(OrderState.FILLED)

        log.log(
            "OrderFilled",
            {"symbol": order.symbol, "qty": order.qty, "side": order.side},
        )

    except ValueError as e:
        order.transition(OrderState.REJECTED)
        log.log(
            "OrderRejected",
            {
                "symbol": order.symbol,
                "qty": order.qty,
                "side": order.side,
                "reason": str(e),
            },
        )


if __name__ == "__main__":
    fix = FixParser()
    risk = RiskEngine(max_order_size=1000, max_position=2000)
    log = Logger(path="events.json")

    raw_messages = [
        "8=FIX.4.2|35=D|55=AAPL|54=1|38=500|40=2|10=128",
        "8=FIX.4.2|35=D|55=MSFT|54=1|38=2000|40=2|10=129",  # should fail order-size
        "8=FIX.4.2|35=D|55=AAPL|54=2|38=300|40=2|10=130",  # sell some
    ]

    for raw in raw_messages:
        handle_message(raw, fix, risk, log)

    log.save()
