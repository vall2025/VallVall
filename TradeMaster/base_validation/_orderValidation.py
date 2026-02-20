"""
All Types and Orders validation
"""


# -----------------------------------------------------------------------------------------------------------------------


class Validator:
    """A powerful universal validation class."""

    ## === Single Value Validations === ##

    @staticmethod
    def is_empty(value, field_name="Value"):
        """Check if a value is empty string."""
        if isinstance(value, str) and value.strip() == "":
            raise ValueError(f"{field_name} cannot be empty string.")

    @staticmethod
    def is_notEmpty(value, field_name="Value"):
        """Check if a value is empty string."""
        if value != "":
            print("value", value)
            raise ValueError(f"{field_name} must be empty string.")

    @staticmethod
    def is_none(value, field_name="Value"):
        """Check if a value is None."""
        if value is None:
            raise ValueError(f"{field_name} is required and cannot be None.")

    @staticmethod
    def is_pos_num(value, field_name="Value"):
        """Check if a value is a positive number (greater than zero)."""
        # If value is not an int or float, try converting it
        if not isinstance(value, (int, float)):
            try:
                value = float(value)  # Convert string numbers like "5.5" or "10" to float
            except ValueError:
                raise TypeError(f"{field_name} must be a valid number greater than zero.")

        # Ensure the number is greater than zero
        if value <= 0:
            raise ValueError(f"{field_name} must be greater than zero.")

    ## === Dict Value Validations === ##

    @staticmethod
    def validate_empty(fields: dict):
        """Validate that all required string fields are non-empty."""
        for field_name, value in fields.items():
            Validator.is_empty(value, field_name)  # Ensure it's not empty string

    @staticmethod
    def validate_notEmpty(fields: dict):
        """Validate that all required string fields are empty."""
        for field_name, value in fields.items():
            Validator.is_notEmpty(value, field_name)  # Ensure it's empty string

    @staticmethod
    def validate_none(fields: dict):
        """Validate that all required fields are not None."""
        for field_name, value in fields.items():
            Validator.is_none(value, field_name)  # Ensure it's not None

    @staticmethod
    def validate_pos_num(fields: dict):
        """Validate that all required numeric fields are positive numbers."""
        for field_name, value in fields.items():
            Validator.is_none(value, field_name)  # Ensure it's not None
            Validator.is_pos_num(value, field_name)  # Ensure it's a valid positive number


# -----------------------------------------------------------------------------------------------------------------------

def validate_place_order(instrumentId, exchange, transactionType, quantity, orderComplexity, product, orderType,
                         price, slTriggerPrice, slLegPrice, targetLegPrice, validity):

    ## === Validator Start === ##

    # Validate empty string and none types
    check_empty = {
        "Instrument Id": instrumentId,
        "Exchange": exchange,
        "Transaction Type": transactionType,
        "Quantity": quantity,
        "Order Complexity": orderComplexity,
        "Product": product,
        "Order Type": orderType,
        "Validity": validity
    }

    if orderType in {"LIMIT", "SL"}:
        check_empty["Price"] = price

    if orderType in {"SL", "SLM"}:
        check_empty["SL Trigger Price"] = slTriggerPrice

    if orderType in {"SL"} and orderComplexity in {"BO", "CO"}:
        check_empty["SL Leg Price"] = slLegPrice

    if orderComplexity in {"BO"}:
        check_empty["Target Leg Price"] = targetLegPrice

    ## === Validator End === ##

    return "Place order validation successful"


def validate_modify_order(brokerOrderId, orderType, price, slTriggerPrice, slLegPrice, targetLegPrice, orderComplexity):
    ## === Validator Start === ##

    # Validate empty string and none types
    check_empty = {
        "Broker Order Number": brokerOrderId,
    }

    if orderType in {"LIMIT", "SL"}:
        check_empty["Price"] = price

    if orderType in {"SL", "SLM"}:
        check_empty["SL Trigger Price"] = slTriggerPrice

    if orderType in {"SL"} and orderComplexity in {"BO", "CO"}:
        check_empty["SL Leg Price"] = slLegPrice

    if orderComplexity in {"BO"}:
        check_empty["Target Leg Price"] = targetLegPrice

    Validator.validate_none(check_empty)
    Validator.validate_empty(check_empty)

    ## === Validator End === ##

    return "Modify order validation successful"


def validate_cancel_order(brokerOrderId):
    ## === Validator Start === ##

    # Validate none types
    Validator.is_none(brokerOrderId, "Broker Order Id")

    # Numeric validation
    Validator.is_empty(brokerOrderId, "Broker Order Id")

    ## === Validator End === ##

    return "Cancel order validation successful."


def validate_exitBracketOrder(brokerOrderId, orderComplexity):
    ## === Validator Start === ##

    # Validate empty string and none types
    check_empty = {
        "Broker Order Id": brokerOrderId,
        "Order Complexity": orderComplexity,
    }

    Validator.validate_none(check_empty)
    Validator.validate_empty(check_empty)

    ## === Validator End === ##

    return "Exit Bracket Order validation successful."


def validate_positionSqrOff(instrumentId, exchange, transactionType, quantity, orderComplexity, product, orderType,
                            price, slTriggerPrice, slLegPrice, targetLegPrice, validity):
    ## === Validator Start === ##

    # Validate empty string and none types
    check_empty = {
        "Instrument Id": instrumentId,
        "Exchange": exchange,
        "Transaction Type": transactionType,
        "Quantity": quantity,
        "Order Complexity": orderComplexity,
        "Product": product,
        "Order Type": orderType,
        "Validity": validity
    }

    if orderType in {"LIMIT", "SL"}:
        check_empty["Price"] = price

    if orderType in {"SL", "SLM"}:
        check_empty["SL Trigger Price"] = slTriggerPrice

    if orderType in {"SL"} and orderComplexity in {"BO", "CO"}:
        check_empty["SL Leg Price"] = slLegPrice

    if orderComplexity in {"BO"}:
        check_empty["Target Leg Price"] = targetLegPrice

    ## === Validator End === ##

    return "Position sqr off validation successful"


def validate_singleOrderMargin(instrumentId, exchange, transactionType, quantity, orderComplexity, product,
                               orderType, price, slTriggerPrice, slLegPrice):
    ## === Validator Start === ##

    # Validate empty string and none types
    check_empty = {
        "Instrument Id": instrumentId,
        "Exchange": exchange,
        "Transaction Type": transactionType,
        "Quantity": quantity,
        "Order Complexity": orderComplexity,
        "Product": product,
        "Order Type": orderType,
    }

    if orderType in {"LIMIT", "SL"}:
        check_empty["Price"] = price

    if orderType in {"SL", "SLM"}:
        check_empty["SL Trigger Price"] = slTriggerPrice

    if orderType in {"SL"} and orderComplexity in {"BO", "CO"}:
        check_empty["SL Leg Price"] = slLegPrice

    Validator.validate_none(check_empty)
    Validator.validate_empty(check_empty)

    ## === Validator End === ##

    return "Single Order Margin validation successful."


def validate_gtt_place_order(quantity, price, gttValue, tradingSymbol):
    ## === Validator Start === ##

    # Validate empty string and none types
    check_empty = {
        "Trading Symbol": tradingSymbol,
        "Quantity": quantity,
        "Price": price,
        "GTT Value": gttValue,
    }

    Validator.validate_none(check_empty)
    Validator.validate_empty(check_empty)

    ## === Validator End === ##

    return "GTT place order validation successful"


def validate_gtt_modify_order(brokerOrderId, quantity, price, gttValue, tradingSymbol):
    ## === Validator Start === ##

    # Validate empty string and none types
    check_empty = {
        "Broker Order Id": brokerOrderId,
        "Trading Symbol": tradingSymbol,
        "Quantity": quantity,
        "Price": price,
        "GTT Value": gttValue,
    }

    Validator.validate_none(check_empty)
    Validator.validate_empty(check_empty)

    ## === Validator End === ##

    return "GTT modify order validation successful"


def validate_basket_margin(quantity, price, tradingSymbol):
    check_empty = {

        "Trading Symbol": tradingSymbol,
        "Quantity": quantity,
        "Price": price,

    }

    Validator.validate_none(check_empty)
    Validator.validate_empty(check_empty)

    return "Basket margin validation successful."

def validate_option_chain(interval, expiry):
    check_empty = {

        "Interval": interval,
        "Expiry Date": expiry,

    }

    Validator.validate_none(check_empty)
    Validator.validate_empty(check_empty)

    return "Option chain validation successful."

def validate_historical_data(resolution):
    check_empty = {
        "ineterval": resolution,
    }

    Validator.validate_none(check_empty)
    Validator.validate_empty(check_empty)

    return "Historical data validation successful."


def validate_chart_history(token, exchange, user, resolution):
    # Validate empty string types
    check_empty = {
        "Token": token,
        "Exchange": exchange,
        "User": user,
        "Resolution": resolution
    }

    Validator.validate_none(check_empty)
    Validator.validate_empty(check_empty)

    ## === Validator End === ##

    return "Chart history validation successful."


def validate_get_margin(tradingSymbol, exchange, orderFlag, product, transType, priceType, orderType):
    ## === Validator Start === ##

    # Validate empty string types
    check_empty = {
        "Trading Symbol": tradingSymbol,
        "Exchange": exchange,
        "Order Flag": orderFlag,
        "Product": product,
        "Trans Type": transType,
        "Price Type": priceType,
        "Order Type": orderType,
    }

    Validator.validate_none(check_empty)
    Validator.validate_empty(check_empty)

    ## === Validator End === ##

    # Validate Transaction Type
    if transType not in {"BUY", "SELL", "B", "S"}:
        raise TypeError("Transaction Type must be one of the following: 'BUY', 'SELL', 'B', or 'S'.")

    return "Get margin validation successful."


def validate_posConvertion(tradingSymbol, exchange, product, prevProduct, transType, posType, quantity):
    ## === Validator Start === ##

    # Validate empty string types
    check_empty = {
        "Trading Symbol": tradingSymbol,
        "Exchange": exchange,
        "Product": product,
        "Prev Product": prevProduct,
        "Trans Type": transType,
        "pos Type": posType,
    }

    Validator.validate_none(check_empty)
    Validator.validate_empty(check_empty)

    # Validate Transaction Type
    if transType not in {"BUY", "SELL", "B", "S"}:
        raise TypeError("Transaction Type must be one of the following: 'BUY', 'SELL', 'B', or 'S'.")

    # Validate numeric types
    check_pos_num = {
        "Quantity": quantity
    }

    Validator.validate_pos_num(check_pos_num)

    ## === Validator End === ##

    return "Position convertion validation successful."

# -----------------------------------------------------------------------------------------------------------------------
