# fix_parser.py

class FixParser:
    """
    Very simple FIX parser for messages like:
    '8=FIX.4.2|35=D|55=AAPL|54=1|38=100|40=2|10=128'
    We treat '|' as the field delimiter.
    """

    def __init__(self, required_tags=None):
        # Required fields for a basic equity order
        # 35 = MsgType, 55 = Symbol, 54 = Side, 38 = OrderQty
        self.required_tags = required_tags or ["35", "55", "54", "38"]

    def parse(self, raw: str) -> dict:
        if not isinstance(raw, str) or not raw:
            raise ValueError("FIX message must be a non-empty string")

        # Support SOH-delimited or '|' delimited messages
        msg = raw.replace("\x01", "|")

        fields = msg.split("|")
        data = {}
        for field in fields:
            if not field:
                continue
            if "=" not in field:
                raise ValueError(f"Malformed FIX field: {field}")
            tag, value = field.split("=", 1)
            data[tag] = value

        # Validate required tags
        for tag in self.required_tags:
            if tag not in data or data[tag] == "":
                raise ValueError(f"Missing required tag: {tag}")

        return data


if __name__ == "__main__":
    msg = "8=FIX.4.2|35=D|55=AAPL|54=1|38=100|40=2|10=128"
    print(FixParser().parse(msg))
