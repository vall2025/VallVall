
class ServiceProps:
    """Service Props class for API settings and endpoints."""

    # Base URL for API requests
    BASE_URL = "https://a3.aliceblueonline.com/"
    API_NAME = "Codifi ProTrade - Python Library"
    CONTRACT_BASE_URL = "https://v2api.aliceblueonline.com/restpy/static/contract_master/"
    WSS_URL = "wss://ant.aliceblueonline.com/order-notify/websocket"

    # Endpoints for authorization
    GET_VENDOR_SESSION = "open-api/od/v1/vendor/getUserDetails"

    # Endpoint for client profile
    GET_PROFILE = "open-api/od/v1/profile/"

    # Endpoint for funds
    GET_FUNDS = "open-api/od/v1/limits/"

    # Endpoints for positions and holdings
    GET_POSITIONS = "open-api/od/v1/positions"
    GET_HOLDINGS = "open-api/od/v1/holdings"

    # Endpoints for position conversion & margin
    POSITION_CONVERSION = "open-api/od/v1/conversion"
    SINGLE_ORDER_MARGIN = "open-api/od/v1/orders/checkMargin"

    # Endpoints for orders
    ORDER_EXECUTE = "open-api/od/v1/orders/placeorder"
    ORDER_MODIFY = "open-api/od/v1/orders/modify"
    ORDER_CANCEL = "open-api/od/v1/orders/cancel"
    EXIT_BRACKET_ORDER = "open-api/od/v1/orders/exit/sno"
    POSITION_SQR_OFF = "open-api/od/v1/orders/positions/sqroff"

    # Endpoints for GTT orders
    GTT_ORDER_EXECUTE = "open-api/od/v1/orders/gtt/execute"
    GTT_ORDER_MODIFY = "open-api/od/v1/orders/gtt/modify"
    GTT_ORDER_CANCEL = "open-api/od/v1/orders/gtt/cancel"

    # Endpoints for orders details
    GET_ORDER_BOOK = "open-api/od/v1/orders/book"
    GET_TRADE_BOOK = "open-api/od/v1/orders/trades"
    GET_ORDER_HISTORY = "open-api/od/v1/orders/history"

    # Endpoints for GTT orders details
    GET_GTT_ORDER_BOOK = "open-api/od/v1/orders/gtt/orderbook"

    # Placeholder for chart history endpoint
    GET_CHART_HISTORY = ""  # Replace with a valid endpoint or remove if not
    GET_UNDERLYING = "obrest/optionChain/getUnderlying"
    GET_UNDERLYING_EXPIRY = "obrest/optionChain/getUnderlyingExp"
    GET_OPTION_CHAIN = "obrest/optionChain/getOptionChain"
    GET_BASKETMARGIN = 'open-api/od/v1/orders/basket/margin'
    GET_HISTORICAL_DATA = 'open-api/od/ChartAPIService/api/chart/history'
    GET_WEBSOCKET_TOKEN = 'open-api/order-notify/websocket'
    GET_WSSSESSION = 'open-api/order-notify/ws/createWsToken'
    GET_INVALIDSESSION_WS = '/open-api/od/v1/profile/invalidateWsSess',
    GET_CREATESESSION_WS = '/open-api/od/v1/profile/createWsSess'
    @staticmethod
    def get_full_url(endpoint):
        """Return full URL for a given endpoint."""
        return f"{ServiceProps.BASE_URL}{endpoint}"







