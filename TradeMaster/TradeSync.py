import base64
import ssl
from time import sleep
import hashlib

import websocket

from utility.library import *

logger = logging.getLogger(__name__)


@dataclass(frozen=False)
class Instrument:
    exchange: Optional[Any]
    token: Optional[Any]
    symbol: Optional[Any]
    trading_symbol: Optional[Any]
    expiry: Optional[Any]
    lot_size: Optional[Any]


"""Predefined Categories"""


class Exchange(enum.Enum):
    NSE = "NSE"
    BSE = "BSE"
    NFO = "NFO"
    BFO = "BFO"
    CDS = "CDS"
    BCD = "BCD"
    NCO = "NCO"
    BCO = "BCO"
    MCX = "MCX"
    INDICES = "INDICES"
    NCDEX = "NCDEX"
    NSE_FO = "nse_fo"
    BSE_FO = "bse_fo"
    MCX_FO = "mcx_fo"


class TransactionType(enum.Enum):
    Buy = "BUY"
    Sell = "SELL"


class OrderComplexity(enum.Enum):
    Regular = 'REGULAR'
    AMO = 'AMO'
    Cover = 'CO'
    Bracket = 'BO'


class ProductType(enum.Enum):
    Normal = "NORMAL"
    Intraday = "INTRADAY"
    Longterm = "LONGTERM"
    Delivery = "DELIVERY"
    BNPL = "BNPL"
    MTF = "MTF"
    GTT = "GTT"
    CNC = "CNC"
    MIS = "MIS"


class OrderType(enum.Enum):
    Limit = 'LIMIT'
    Market = 'MARKET'
    StopLoss = 'SL'
    StopLossMarket = 'SLM'


class PositionType(enum.Enum):
    posDAY = "DAY"
    posNET = "NET"
    posIOC = "IOC"


class OrderSource(enum.Enum):
    WEB = 'WEB'
    API = 'API'
    MOB = 'MOB'


class TradeHub:
    """
    TradeHub SDK Class - Handles all trade-related API operations.
    """

    # Implement for Websocket

    ENC = None
    ws = None
    subscriptions = None
    __subscribe_callback = None
    __subscribers = None
    script_subscription_instrument = []
    ws_connection = False
    # response = requests.get(base_url);
    # Getscrip URI
    __ws_thread = None
    __stop_event = None
    market_depth = None

    def __init__(
            self,
            user_id: Union[str, int],
            auth_code: str,
            secret_key: str,
            base_url: str = None,
            session_id: str = None,
            __on_error: str = None,
            __on_disconnect: str = None,
            __on_open: str = None,

    ):
        # Initialize the values
        self._user_id = user_id.upper()
        self._auth_code = auth_code
        self._secret_key = secret_key
        self._session_id = session_id
        self.__on_error = None
        self.__on_disconnect = None
        self.__on_open = None
        # Base URL for API requests
        self._base_url = base_url or ServiceProps.BASE_URL
        self._api_name = ServiceProps.API_NAME
        self._base_url_contract = ServiceProps.CONTRACT_BASE_URL
        self._version = SettingsProps.PIP_VERSION
        self._wss_url = ServiceProps.WSS_URL
        self.websocket_url = 'wss://ws1.aliceblueonline.com/NorenWS/'

        # URL Mappings
        self._endpoints = {
            # API User Authorization Part
            "getSession": ServiceProps.GET_VENDOR_SESSION,

            # User & Portfolio Management Part
            "getProfile": ServiceProps.GET_PROFILE,
            "getFunds": ServiceProps.GET_FUNDS,
            "getPositions": ServiceProps.GET_POSITIONS,
            "getHoldings": ServiceProps.GET_HOLDINGS,

            # Position Conversion & Margin Management Part
            "posConversion": ServiceProps.POSITION_CONVERSION,
            "positionSqrOff": ServiceProps.POSITION_SQR_OFF,
            "SingleOrdMargin": ServiceProps.SINGLE_ORDER_MARGIN,

            # Order & Trade Management Part
            "ordExecute": ServiceProps.ORDER_EXECUTE,
            "ordModify": ServiceProps.ORDER_MODIFY,
            "ordCancel": ServiceProps.ORDER_CANCEL,
            "ordExitBracket": ServiceProps.EXIT_BRACKET_ORDER,

            # GTT Order & Trade Management Part
            "ordGTT_Execute": ServiceProps.GTT_ORDER_EXECUTE,
            "ordGTT_Modify": ServiceProps.GTT_ORDER_MODIFY,
            "ordGTT_Cancel": ServiceProps.GTT_ORDER_CANCEL,

            # Order & Trade History Retrieval Part
            "getOrderbook": ServiceProps.GET_ORDER_BOOK,
            "getTradebook": ServiceProps.GET_TRADE_BOOK,
            "getOrdHistory": ServiceProps.GET_ORDER_HISTORY,

            # GTT Orders Retrieval Part
            "getGTTOrderbook": ServiceProps.GET_GTT_ORDER_BOOK,

            # Chart & Historical Data Part
            "getChartHistory": ServiceProps.GET_CHART_HISTORY,

            # GetUnderlying for Optionchain
            "getUnderlying": ServiceProps.GET_UNDERLYING,
            "getUnderlyingExpiry": ServiceProps.GET_UNDERLYING_EXPIRY,
            "getOptionChain": ServiceProps.GET_OPTION_CHAIN,
            "getBasketMargin": ServiceProps.GET_BASKETMARGIN,
            "getHistoricalData": ServiceProps.GET_HISTORICAL_DATA,
            "getWStoken": ServiceProps.GET_WEBSOCKET_TOKEN,
            "getWSSession": ServiceProps.GET_WSSSESSION,
            "getInvalidSession": ServiceProps.GET_INVALIDSESSION_WS,
            "getCreateSession": ServiceProps.GET_CREATESESSION_WS
        }

    """API Methods Declaration Part"""

    def _init_session(self):
        return RequestHandler(session_token=self.sessionAuthorization())

    def _init_get(self, _endpoints_key, data=None, params=None, pathParameter=None, wss_url=None):
        """Send a GET request to the specified endpoint key with optional path parameter."""
        # Construct the URL safely
        endpoint = self._endpoints.get(_endpoints_key, "")
        if not endpoint:
            return self._errorResponse(message=f"Invalid endpoint key: {_endpoints_key}")
        if wss_url == "websocket":
            url = self._wss_url + endpoint
        else:
            url = self._base_url + endpoint
        # Append the path parameter if provided
        if pathParameter:
            # Ensure there's a slash between the endpoint and the path parameter
            if not url.endswith('/'):
                url += '/'
            url += str(pathParameter)  # Convert to string in case it's not

        api = self._init_session()
        return api.request(url=url, method="GET", data=data, params=params)

    def _init_post(self, _endpoints_key, data=None, params=None, pathParameter=None):
        """Send a POST request to the specified endpoint key with optional path parameter."""
        # Construct the URL safely
        endpoint = self._endpoints.get(_endpoints_key, "")
        if not endpoint:
            return self._errorResponse(message=f"Invalid endpoint key: {_endpoints_key}")

        url = self._base_url + endpoint
        # Append the path parameter if provided
        if pathParameter:
            # Ensure there's a slash between the endpoint and the path parameter
            if not url.endswith('/'):
                url += '/'
            url += str(pathParameter)  # Convert to string in case it's not

        api = self._init_session()
        print(api)
        return api.request(url=url, method="POST", data=data, params=params)

    def _init_put(self, _endpoints_key, data=None, params=None, pathParameter=None):
        """Send a PUT request to the specified endpoint key with optional path parameter."""
        # Construct the URL safely
        endpoint = self._endpoints.get(_endpoints_key, "")
        if not endpoint:
            return self._errorResponse(message=f"Invalid endpoint key: {_endpoints_key}")

        url = self._base_url + endpoint
        # Append the path parameter if provided
        if pathParameter:
            # Ensure there's a slash between the endpoint and the path parameter
            if not url.endswith('/'):
                url += '/'
            url += str(pathParameter)  # Convert to string in case it's not

        api = self._init_session()
        return api.request(url=url, method="PUT", data=data, params=params)

    def _init_del(self, _endpoints_key, data=None, params=None, pathParameter=None):
        """Send a DELETE request to the specified endpoint key with optional path parameter."""
        # Construct the URL safely
        endpoint = self._endpoints.get(_endpoints_key, "")
        if not endpoint:
            return self._errorResponse(message=f"Invalid endpoint key: {_endpoints_key}")

        url = self._base_url + endpoint
        # Append the path parameter if provided
        if pathParameter:
            # Ensure there's a slash between the endpoint and the path parameter
            if not url.endswith('/'):
                url += '/'
            url += str(pathParameter)  # Convert to string in case it's not

        api = self._init_session()
        return api.request(url=url, method="DELETE", data=data, params=params)

    """Response Handler Part"""

    @staticmethod
    def _response(message: str, encKey=None):
        return {'stat': 'Ok', 'emsg': message, 'encKey': encKey}

    @staticmethod
    def _errorResponse(message: str, encKey=None):
        return {'stat': 'Not_ok', 'emsg': message, 'encKey': encKey}

    """API User Authorization Part"""

    def get_session_id(self, check_sum=None, session_id=None):
        """
        Retrieves or generates a session ID (checksum) for user authentication.

        Args:
            check_sum (str, optional): An existing checksum. If provided and valid, it will be used.
            session_id (str, optional): An existing session token. If provided and valid, it will be used.

        Returns:
            dict: A dictionary containing the 'userSession' value used for API requests.
        """

        if session_id and session_id.strip():
            self._session_id = session_id.strip()
            return {"userSession": self._session_id}

        try:
            if not check_sum or not check_sum.strip():
                check_sum = generate_checksum(
                    user_id=self._user_id,
                    auth_Code=self._auth_code,
                    secret_key=self._secret_key
                )
            else:
                check_sum = check_sum.strip()

            data = {'checkSum': check_sum}
            response = self._init_post(_endpoints_key="getSession", data=data)
            print(response)

            self._session_id = None

            if response and (response.get('status') == 'Ok' or response.get('stat') == 'Ok'):
                result = response.get('result')
                if response.get('status') == 'Ok' and isinstance(result, list) and len(result) > 0 and isinstance(
                        result[0], dict):
                    access_token = result[0].get('accessToken')
                    if access_token:
                        self._session_id = access_token
                        return {"userSession": self._session_id}
                elif response.get('stat') == 'Ok' and response.get('userSession'):
                    self._session_id = response.get('userSession')
                    return {"userSession": self._session_id}
                else:
                    # Handle cases where 'result' is missing or empty
                    return self._errorResponse(message="Session ID not found in response.")
            else:
                return self._errorResponse(message=response.get('message') or response.get('emsg'))


        except Exception as e:
            return self._errorResponse(message=f"Error generating session: {str(e)}")

    def sessionAuthorization(self):
        # Return the Bearer token if _session_id is available, else return empty string
        if self._session_id:
            return f"Bearer {self._session_id}"
        else:
            return ""

    """Contract Master Management Part"""

    def get_contract_master(self, exchange: Union[str, Exchange]):
        """
        Downloads the contract master file for the given exchange.

        Parameters:
            exchange (str): Exchange name (only 'INDICES' is supported).

        Returns:
            dict: Success message indicating whether today's or previous day's contract file is saved.

        Note:
            Contract file is updated daily after 08:00 AM.
        """

        # Handle Both Enum and String
        exchange = getattr(exchange, 'value', exchange)

        # Validate exchange parameter
        if exchange not in ('INDICES', 'NCDEX') and (exchange is None or len(exchange) != 3):
            return self._errorResponse(message="Invalid Exchange parameter")

        # Print reminder about contract file update time
        print(
            "Reminder: The contract master file is updated daily after 08:00 AM. Before this time, the previous day's contract file will be available for download.")

        # Check if the current time is after 08:00 AM
        if datetime.now().time() >= time(8, 0):
            def _downloadReq(exchange_url):
                url = self._base_url_contract + exchange_url
                try:
                    response = requests.get(url, timeout=30)
                    return response
                except requests.RequestException:
                    return None

            # Try possible variations in priority order
            candidates = [
                exchange.lower(),
                exchange.lower() + ".csv",
                exchange.lower() + ".CSV",
                exchange.upper(),
                exchange.upper() + ".csv",
                exchange.upper() + ".CSV"
            ]

            response = None
            for candidate in candidates:
                response = _downloadReq(candidate)
                if response and response.status_code == 200:
                    # Save file only once a valid response is received
                    with open(f"{exchange.upper()}.csv", "w") as f:
                        f.write(response.text)
                    return self._response(message="Today's contract file downloaded")

            return self._response(message="Failed to download today's contract file")

        else:
            return self._response(message="Previous day contract file saved")

    """Script Search Part"""

    def get_instrument(self, exchange: Union[str, Exchange], symbol: Union[str] = None, token: Union[str, int] = None):
        """
        Get instrument details using symbol or token for a given exchange.

        Parameters:
            exchange (str): Exchange name (e.g., 'NSE', 'BSE', 'NFO', 'INDICES').
            symbol (str, optional): Symbol or trading symbol of the instrument.
            token (str or int, optional): Token of the instrument.

        Returns:
            Instrument: Instrument details if found.

        Error:
            Returns error response if symbol/token not found or contract file is missing.
        """

        # Handle Both Enum and String
        exchange = getattr(exchange, 'value', exchange)

        if not symbol and not token:
            return self._errorResponse(message="Either symbol or token must be provided")

        try:
            contract = contract_read(exchange)
        except OSError as e:
            if e.errno == 2:
                self.get_contract_master(exchange)
                contract = contract_read(exchange)
            else:
                return self._errorResponse(message=str(e))

        if exchange == 'INDICES':
            filter_contract = contract[contract['token'] == token] if token else contract[
                contract['symbol'] == symbol.upper()]

            if filter_contract.empty:
                return self._errorResponse(message="The symbol is not available in this exchange")

            filter_contract = filter_contract.reset_index(drop=True)

            inst = Instrument(
                exchange=filter_contract.at[0, 'exch'],
                token=filter_contract.at[0, 'token'],
                symbol=filter_contract.at[0, 'symbol'],
                trading_symbol='',
                expiry='',
                lot_size=''
            )

            return inst

        else:
            filter_contract = contract[contract['Token'] == token] if token else contract[
                (contract['Symbol'] == symbol.upper()) | (contract['Trading Symbol'] == symbol.upper())]

            if filter_contract.empty:
                return self._errorResponse(message="The symbol is not available in this exchange")

            filter_contract = filter_contract.reset_index(drop=True)

            expiry = (
                filter_contract.at[0, 'Expiry Date']
                if 'Expiry Date' in filter_contract.columns
                else (
                    filter_contract.at[0, 'expiry_date']
                    if 'expiry_date' in filter_contract.columns
                    else ''
                )
            )

            inst = Instrument(
                exchange=filter_contract.at[0, 'Exch'],
                token=filter_contract.at[0, 'Token'],
                symbol=filter_contract.at[0, 'Symbol'],
                trading_symbol=filter_contract.at[0, 'Trading Symbol'],
                expiry=expiry,
                lot_size=filter_contract.at[0, 'Lot Size']
            )

            return inst

    def get_instrument_for_fno(self, exchange: Union[str, Exchange], symbol: Union[str], expiry_date: Union[str],
                               is_fut=True,
                               strike=None, is_CE=False):
        """
        Fetches the Instrument object(s) for the given F&O parameters.

        :param exchange: Exchange code like 'NFO', 'CDS', etc.
        :param symbol: Trading symbol.
        :param expiry_date: Contract expiry date (string in YYYY-MM-DD format).
        :param is_fut: Boolean flag to indicate if it's a future contract.
        :param strike: Strike price (required for options).
        :param is_CE: Boolean to indicate Call (True) or Put (False) option.

        :return: Single Instrument object or list of Instrument objects.
        """

        # Handle Both Enum and String
        global filter_contract
        exchange = getattr(exchange, 'value', exchange)
        # print(exchange)

        # Validation part
        if exchange not in ['NFO', 'CDS', 'MCX', 'BFO', 'BCD']:
            return self._errorResponse(message="The provided exchange is not supported.")

        if not symbol:
            return self._errorResponse(message="Missing symbol: Please provide a valid symbol.")

        if not isinstance(is_CE, bool):
            return self._errorResponse(message="'is_CE' must be a boolean value (True or False)")

        if not isinstance(is_fut, bool):
            return self._errorResponse(message="'is_fut' must be a boolean value (True or False)")

        if strike is not None and is_fut is True:
            return self._errorResponse(message="Futures contract should not have a strike price")

        # Manipulation part
        edate_format = '%d-%m-%Y' if exchange == 'CDS' else '%Y-%m-%d'
        option_type = "CE" if is_CE is True else "PE"  # Ternary operation for simplicity

        # File read part
        try:
            expiry_date = datetime.strptime(expiry_date, "%Y-%m-%d").date()

            expiry_str = expiry_date.strftime(edate_format)

        except ValueError as e:
            return self._errorResponse(message=str(e))

        try:
            contract = contract_read(exchange)

        except OSError as e:
            if e.errno == 2:
                self.get_contract_master(exchange)
                contract = contract_read(exchange)

            else:
                return self._errorResponse(message=str(e))

        # Base Filter
        base_filter = (contract['Exch'] == exchange) & (
                (contract['Symbol'] == symbol) | (contract['Trading Symbol'] == symbol)
        )

        # Handle options
        if is_fut is False:

            if strike:
                contract['Strike Price'] = contract['Strike Price'].astype(float)
                filter_contract = contract[base_filter & (
                        contract['Option Type'] == option_type) & (
                                                   (contract['Strike Price'] == int(strike)) | (
                                                   contract['Strike Price'] == strike)) & (
                                                   contract['Expiry Date'] == expiry_str)]

            else:
                filter_contract = contract[base_filter & (
                        contract['Option Type'] == option_type) & (
                                                   contract['Expiry Date'] == expiry_str)]
                print(filter_contract)

        # Handle futures
        elif is_fut is True:

            filter_contract = contract[base_filter & (
                    contract['Option Type'] == 'XX') & (
                                               contract['Expiry Date'] == expiry_str)]

        if filter_contract.empty:
            return self._errorResponse(message="The symbol is not available in this exchange")

        else:
            inst = []
            token = []
            filter_contract = filter_contract.reset_index(drop=True)

            for i in range(len(filter_contract)):

                token_value = filter_contract.at[i, 'Token']
                if pd.notnull(token_value):
                    token_value = int(token_value) if isinstance(token_value,
                                                                 (np.int64, np.float64, float)) else token_value

                lotSize_value = filter_contract.at[i, 'Lot Size']
                if pd.notnull(lotSize_value):
                    lotSize_value = int(lotSize_value) if isinstance(lotSize_value,
                                                                     (np.int64, np.float64, float)) else lotSize_value

                if token_value not in token:
                    token.append(token_value)
                    inst.append(Instrument(filter_contract['Exch'][i], token_value,
                                           filter_contract['Symbol'][i], filter_contract['Trading Symbol'][i],
                                           filter_contract['Expiry Date'][i], lotSize_value))

            return inst[0] if len(inst) == 1 else inst

    """User & Portfolio Management Part"""

    def get_profile(self):
        """Fetch user profile information."""
        profile = self._init_get(_endpoints_key="getProfile")
        return profile

    def get_funds(self):
        """Fetch user funds information."""
        funds = self._init_get(_endpoints_key="getFunds")
        return funds

    def get_positions(self):
        """Fetch user positions information."""
        Positions = self._init_get(_endpoints_key="getPositions")
        return Positions

    def get_holdings(self, product=None):
        """Fetch user holdings information."""
        product = getattr(product, 'value', product) if product else product
        Holdings = self._init_get(_endpoints_key="getHoldings", pathParameter=product)
        return Holdings

    """Order & Trade Management Part"""

    def placeOrder(self, transactionType, quantity, orderComplexity, product, orderType, price, slTriggerPrice,
                   slLegPrice, targetLegPrice, validity, trailingSlAmount=None, disclosedQuantity=None,
                   marketProtectionPercent=None, apiOrderSource=None, algoId=None, orderTag=None, instrument=None,
                   instrumentId=None, exchange: Optional[Union[str, Exchange]] = None):
        """
        # Mandatory
        :param instrumentId:
        :param exchange:
        :param transactionType:
        :param quantity:
        :param orderComplexity:
        :param product:
        :param orderType:
        :param validity:

        # Conditionally Required
        :param price:
        :param slTriggerPrice:
        :param slLegPrice:
        :param targetLegPrice:

        # Optional
        :param trailingSlAmount:
        :param disclosedQuantity:
        :param marketProtectionPercent:
        :param apiOrderSource:
        :param algoId:
        :param orderTag:
        :param instrument:

        :return:
        """

        # Validate instrument or token
        if instrument is not None:
            if not isinstance(instrument, Instrument):
                raise TypeError("Instrument must be of type Instrument")
            exchange = exchange or instrument.exchange
            instrumentId = instrumentId or instrument.token

        # Handle Both Enum and String
        transactionType = getattr(transactionType, 'value', transactionType)
        orderComplexity = getattr(orderComplexity, 'value', orderComplexity)
        product = getattr(product, 'value', product)
        orderType = getattr(orderType, 'value', orderType)
        validity = getattr(validity, 'value', validity)
        exchange = getattr(exchange, 'value', exchange)

        # Request Validate Part
        validate_place_order(instrumentId=instrumentId, exchange=exchange, transactionType=transactionType,
                             quantity=quantity, orderComplexity=orderComplexity, product=product, orderType=orderType,
                             price=price, slTriggerPrice=slTriggerPrice, slLegPrice=slLegPrice,
                             targetLegPrice=targetLegPrice, validity=validity)

        # Build data based on the format
        data = [
            {
                "instrumentId": instrumentId,
                "exchange": exchange,
                "transactionType": transactionType,
                "quantity": quantity,
                "orderComplexity": orderComplexity,
                "product": product,
                "orderType": orderType,
                "price": price,
                "slTriggerPrice": slTriggerPrice,
                "slLegPrice": slLegPrice,
                "trailingSlAmount": trailingSlAmount,
                "targetLegPrice": targetLegPrice,
                "validity": validity,
                "disclosedQuantity": disclosedQuantity,
                "marketProtectionPercent": marketProtectionPercent,
                "apiOrderSource": apiOrderSource,
                "algoId": algoId,
                "orderTag": orderTag
            }
        ]
        print(data)

        # API call
        ordExecuteResp = self._init_post(_endpoints_key="ordExecute", data=data)

        return ordExecuteResp

    def modifyOrder(self, brokerOrderId, price, slTriggerPrice, slLegPrice,
                    targetLegPrice, quantity=None, orderType=None, trailingSLAmount=None,
                    validity=None, disclosedQuantity=None, marketProtectionPrecent=None, orderComplexity=None,
                    deviceId=None):
        """
        # Mandatory
        :param brokerOrderId:

        # Conditionally Required:
        :param price:
        :param slTriggerPrice:
        :param slLegPrice:
        :param targetLegPrice:

        # Optional
        :param quantity:
        :param orderType:
        :param trailingSLAmount:
        :param validity:
        :param disclosedQuantity:
        :param marketProtectionPrecent:
        :param orderComplexity:

        :return:
        """

        # Handle Both Enum and String
        orderType = getattr(orderType, 'value', orderType) if orderType is not None else None
        validity = getattr(validity, 'value', validity) if validity is not None else None

        # Request Validate Part
        validate_modify_order(brokerOrderId=brokerOrderId, orderType=orderType, price=price,
                              slTriggerPrice=slTriggerPrice,
                              slLegPrice=slLegPrice, targetLegPrice=targetLegPrice, orderComplexity=orderComplexity)

        # Build data based on the format
        data = {
            "brokerOrderId": brokerOrderId,
            "quantity": quantity if quantity else "",
            "orderType": orderType if orderType else "",
            "slTriggerPrice": slTriggerPrice,
            "price": price,
            "slLegPrice": slLegPrice,
            "trailingSLAmount": trailingSLAmount if trailingSLAmount else "",
            "targetLegPrice": targetLegPrice,
            "validity": validity if validity else "",
            "disclosedQuantity": disclosedQuantity if disclosedQuantity else "",
            "marketProtectionPrecent": marketProtectionPrecent if marketProtectionPrecent else "",
            "deviceId": deviceId if marketProtectionPrecent else ""
        }

        # API call
        ordModifyResp = self._init_post(_endpoints_key="ordModify", data=data)
        print(ordModifyResp)
        return ordModifyResp

    def cancelOrder(self, brokerOrderId):
        # Validate brokerOrderId
        validate_cancel_order(brokerOrderId)

        # Build data based on the format
        data = {
            "brokerOrderId": brokerOrderId,
        }

        # API call
        ordCancelResp = self._init_post(_endpoints_key="ordCancel", data=data)
        return ordCancelResp

    def exitBracketOrder(self, brokerOrderId, orderComplexity):
        """
        # Mandatory
        :param brokerOrderId:
        :param orderComplexity:

        :return:
        """

        # Handle Both Enum and String
        orderComplexity = getattr(orderComplexity, 'value', orderComplexity)

        # Request Validate Part
        validate_exitBracketOrder(brokerOrderId=brokerOrderId,
                                  orderComplexity=orderComplexity)

        # Build data based on the format
        data = [
            {
                "brokerOrderId": brokerOrderId,
                "orderComplexity": orderComplexity,
            }
        ]

        # API call
        ordExitBracketResp = self._init_post(_endpoints_key="ordExitBracket", data=data)
        return ordExitBracketResp

    def positionSqrOff(self, transactionType, quantity, orderComplexity, product, orderType, price, slTriggerPrice,
                       slLegPrice, targetLegPrice, validity, trailingSlAmount=None, disclosedQuantity=None,
                       marketProtectionPercent=None, apiOrderSource=None, algoId=None, orderTag=None, instrument=None,
                       instrumentId=None, exchange: Optional[Union[str, Exchange]] = None):
        """
        # Mandatory
        :param instrumentId:
        :param exchange:
        :param transactionType:
        :param quantity:
        :param orderComplexity:
        :param product:
        :param orderType:
        :param validity:

        # Conditionally Required
        :param price:
        :param slTriggerPrice:
        :param slLegPrice:
        :param targetLegPrice:

        # Optional
        :param trailingSlAmount:
        :param disclosedQuantity:
        :param marketProtectionPercent:
        :param apiOrderSource:
        :param algoId:
        :param orderTag:
        :param instrument:

        :return:
        """

        # Validate instrument or token
        if instrument is not None:
            if not isinstance(instrument, Instrument):
                raise TypeError("Instrument must be of type Instrument")
            exchange = exchange or instrument.exchange
            instrumentId = instrumentId or instrument.token

        # Handle Both Enum and String
        transactionType = getattr(transactionType, 'value', transactionType)
        orderComplexity = getattr(orderComplexity, 'value', orderComplexity)
        product = getattr(product, 'value', product)
        orderType = getattr(orderType, 'value', orderType)
        validity = getattr(validity, 'value', validity)
        exchange = getattr(exchange, 'value', exchange)

        # Request Validate Part
        validate_place_order(instrumentId=instrumentId, exchange=exchange, transactionType=transactionType,
                             quantity=quantity, orderComplexity=orderComplexity, product=product, orderType=orderType,
                             price=price, slTriggerPrice=slTriggerPrice, slLegPrice=slLegPrice,
                             targetLegPrice=targetLegPrice, validity=validity)

        # Build data based on the format
        data = [
            {
                "instrumentId": instrumentId,
                "exchange": exchange,
                "transactionType": transactionType,
                "quantity": quantity,
                "orderComplexity": orderComplexity,
                "product": product,
                "orderType": orderType,
                "price": price,
                "slTriggerPrice": slTriggerPrice,
                "slLegPrice": slLegPrice,
                "trailingSlAmount": trailingSlAmount,
                "targetLegPrice": targetLegPrice,
                "validity": validity,
                "disclosedQuantity": disclosedQuantity,
                "marketProtectionPercent": marketProtectionPercent,
                "apiOrderSource": apiOrderSource,
                "algoId": algoId,
                "orderTag": orderTag
            }
        ]
        print(data)
        # API call
        ordExecuteResp = self._init_post(_endpoints_key="positionSqrOff", data=data)
        return ordExecuteResp

    def singleOrderMargin(self, transactionType, quantity, orderComplexity, product, orderType, price,
                          slTriggerPrice, slLegPrice, instrument=None, instrumentId=None,
                          exchange: Optional[Union[str, Exchange]] = None):
        """
        # Mandatory
        :param instrumentId:
        :param exchange:
        :param transactionType:
        :param quantity:
        :param orderComplexity:
        :param product:
        :param orderType:

        # Conditionally Required
        :param price:
        :param slTriggerPrice:
        :param slLegPrice:

        :return:
        """

        # Validate instrument or token
        if instrument is not None:
            if not isinstance(instrument, Instrument):
                raise TypeError("Instrument must be of type Instrument")
            exchange = exchange or instrument.exchange
            instrumentId = instrumentId or instrument.token

        # Handle Both Enum and String
        transactionType = getattr(transactionType, 'value', transactionType)
        orderComplexity = getattr(orderComplexity, 'value', orderComplexity)
        product = getattr(product, 'value', product)
        orderType = getattr(orderType, 'value', orderType)
        exchange = getattr(exchange, 'value', exchange)

        # Request Validate Part
        validate_singleOrderMargin(instrumentId=instrumentId, exchange=exchange, transactionType=transactionType,
                                   quantity=quantity, orderComplexity=orderComplexity, product=product,
                                   orderType=orderType, price=price, slTriggerPrice=slTriggerPrice,
                                   slLegPrice=slLegPrice)

        # Build data based on the format
        data = {
            "instrumentId": instrumentId,
            "exchange": exchange,
            "transactionType": transactionType,
            "quantity": quantity,
            "orderComplexity": orderComplexity,
            "product": product,
            "orderType": orderType,
            "price": price,
            "slTriggerPrice": slTriggerPrice,
            "slLegPrice": slLegPrice,
        }

        # API call
        SingleOrdMarginResp = self._init_post(_endpoints_key="SingleOrdMargin", data=data)
        return SingleOrdMarginResp

    """GTT Order & Trade Management Part"""

    def GTT_placeOrder(self, transactionType, quantity, orderComplexity, product, orderType, price, gttValue, validity,
                       instrument=None, instrumentId=None, exchange: Optional[Union[str, Exchange]] = None,
                       tradingSymbol=None):
        """
        # Mandatory
        :param instrumentId:
        :param tradingSymbol:
        :param exchange:
        :param transactionType:
        :param quantity:
        :param orderComplexity:
        :param product:
        :param orderType:
        :param price:
        :param gttValue:
        :param validity:

        :return:
        """

        # Validate instrument or token
        if instrument is not None:
            if not isinstance(instrument, Instrument):
                raise TypeError("Instrument must be of type Instrument")
            exchange = exchange or instrument.exchange
            instrumentId = instrumentId or instrument.token
            tradingSymbol = tradingSymbol or instrument.trading_symbol

        # Handle Both Enum and String
        transactionType = getattr(transactionType, 'value', transactionType)
        orderComplexity = getattr(orderComplexity, 'value', orderComplexity)
        product = getattr(product, 'value', product)
        orderType = getattr(orderType, 'value', orderType)
        validity = getattr(validity, 'value', validity)
        exchange = getattr(exchange, 'value', exchange)

        # Request Validate Part
        validate_gtt_place_order(quantity=quantity,
                                 price=price, gttValue=gttValue, tradingSymbol=tradingSymbol)

        # Build data based on the format
        data = {
            "instrumentId": instrumentId,
            "tradingSymbol": tradingSymbol,
            "exchange": exchange,
            "transactionType": transactionType,
            "quantity": quantity,
            "orderComplexity": orderComplexity,
            "product": product,
            "orderType": orderType,
            "price": price,
            "gttValue": gttValue,
            "validity": validity,
        }

        # API call
        ordGTT_ExecuteResp = self._init_post(_endpoints_key="ordGTT_Execute", data=data)
        print(ordGTT_ExecuteResp)
        return ordGTT_ExecuteResp

    def GTT_modifyOrder(self, brokerOrderId, quantity, orderComplexity, product, orderType, price, gttValue,
                        validity, instrument=None, instrumentId=None, exchange: Optional[Union[str, Exchange]] = None,
                        tradingSymbol=None):
        """
        # Mandatory
        :param brokerOrderId:
        :param instrumentId:
        :param tradingSymbol:
        :param exchange:
        :param quantity:
        :param orderComplexity:
        :param product:
        :param orderType:
        :param price:
        :param validity:

        :return:
        """

        # Validate instrument or token
        if instrument is not None:
            if not isinstance(instrument, Instrument):
                raise TypeError("Instrument must be of type Instrument")
            exchange = exchange or instrument.exchange
            instrumentId = instrumentId or instrument.token
            tradingSymbol = tradingSymbol or instrument.trading_symbol

        # Handle Both Enum and String
        orderComplexity = getattr(orderComplexity, 'value', orderComplexity)
        product = getattr(product, 'value', product)
        orderType = getattr(orderType, 'value', orderType)
        validity = getattr(validity, 'value', validity)
        exchange = getattr(exchange, 'value', exchange)

        # Request Validate Part
        validate_gtt_modify_order(brokerOrderId=brokerOrderId, quantity=quantity,
                                  price=price, gttValue=gttValue, tradingSymbol=tradingSymbol)

        # Build data based on the format
        data = {
            "brokerOrderId": brokerOrderId,
            "instrumentId": instrumentId,
            "tradingSymbol": tradingSymbol,
            "exchange": exchange,
            "quantity": quantity,
            "orderComplexity": orderComplexity,
            "product": product,
            "orderType": orderType,
            "price": price,
            "gttValue": gttValue,
            "validity": validity,
        }

        # API call
        ordGTT_ModifyResp = self._init_post(_endpoints_key="ordGTT_Modify", data=data)
        return ordGTT_ModifyResp

    def GTT_cancelOrder(self, brokerOrderId):
        # Validate brokerOrderId
        validate_cancel_order(brokerOrderId)

        # Build data based on the format
        data = {
            "brokerOrderId": brokerOrderId,
        }

        # API call
        ordGTT_CancelResp = self._init_post(_endpoints_key="ordGTT_Cancel", data=data)
        return ordGTT_CancelResp

    """Order & Trade History Retrieval"""

    def get_orderbook(self):
        """Fetch user Order book information."""
        Orderbook = self._init_get(_endpoints_key="getOrderbook")
        return Orderbook

    def get_tradebook(self):
        """Fetch user Trade book information."""
        Tradebook = self._init_get(_endpoints_key="getTradebook")
        return Tradebook

    def get_orderHistory(self, brokerOrderId):
        """Fetch user Order History with Order Number information."""
        # Validate brokerOrderId
        if not brokerOrderId or not isinstance(brokerOrderId, str) or brokerOrderId.strip() == '':
            raise TypeError("Order Number must be a non-empty string and cannot be None")

        # Build data based on the format
        data = {
            "brokerOrderId": brokerOrderId,
        }

        # API call
        getOrdHistoryResp = self._init_post(_endpoints_key="getOrdHistory", data=data)
        return getOrdHistoryResp

    """GTT Order & Trade History Retrieval"""

    def get_GTT_Orderbook(self):
        """Fetch user GTT Order book information."""
        GTT_Orderbook = self._init_get(_endpoints_key="getGTTOrderbook")
        return GTT_Orderbook

    ### NEW API HAS BEEN ADDED BELOW ###

    def get_Underlying(self, exchange: Union[str, Exchange]):
        # Get Underlying
        """
           # Mandatory
           :param exchange:
           :return:
               """
        exchange = getattr(exchange, 'value', exchange)
        payload = {
            "exch": exchange
        }

        # API call
        getUnderlyingResp = self._init_post(_endpoints_key="getUnderlying", data=payload)
        return getUnderlyingResp

    def get_Underlying_expiry(self, exchange: Union[str, Exchange], underlying):
        # Get Underlying Expiry
        """
           # Mandatory
           :param exchange:
           :param underlying:

           :return:
               """
        exchange = getattr(exchange, 'value', exchange)
        payload = {
            "exch": exchange,
            "underlying": underlying
        }

        # API call
        getUnderlyingexpiryResp = self._init_post(_endpoints_key="getUnderlyingExpiry", data=payload)
        return getUnderlyingexpiryResp

    def get_Option_chain(self, exchange: Union[str, Exchange], underlying, interval, expiry):
        # Get Option Chain data
        """
           # Mandatory
           :param exchange:
           :param underlying:
           :param interval:
           :param expiry:
           :return:
               """
        exchange = getattr(exchange, 'value', exchange)
        validate_option_chain(interval, expiry)
        payload = {
            "exch": exchange,
            "underlying": underlying,
            "interval": interval,
            "expiry": expiry
        }

        # API call
        getOptionchainResp = self._init_post(_endpoints_key="getOptionChain", data=payload)
        return getOptionchainResp

    def get_BasketMargin(self, quantity, product, price, priceType,
                         triggerPrice, transactionType,
                         exchange: Optional[Union[str, Exchange]] = None, tradingSymbol=None):
        """
        # Mandatory
        :param quantity:
        :param product:
        :param priceType:
        :param tradingSymbol:
        :param exchange:

        # Conditionally Required
        :param price:
        :param triggerPrice:


        :return:
        """

        product = getattr(product, 'value', product)
        exchange = getattr(exchange, 'value', exchange)
        transactionType = getattr(transactionType, 'value', transactionType)

        validate_basket_margin(quantity=quantity,
                               price=price, tradingSymbol=tradingSymbol)

        # Build data based on the format
        data = [{
            "exchange": exchange,
            "qty": quantity,
            "product": product,
            "price": price,
            "priceType": priceType,
            "TriggerPrice": triggerPrice,
            "transType": transactionType,
            "tradingSymbol": tradingSymbol
        }]

        # API call
        BasketMarginResp = self._init_post(_endpoints_key="getBasketMargin", data=data)
        return BasketMarginResp

    def get_HistoricalData(self, instrument, resolution, from_datetime, to_datetime,indices=False):
        """
        # Mandatory
        :param instrumentId:
        :param resolution:

        # Conditionally Required
        :param from_datetime:
        :param to_datetime:

        :return:
        """

        def ensure_datetime(value):
            if isinstance(value, str):
                return datetime.strptime(value, "%Y%m%d")
            return value  # already datetime

        from_datetime = ensure_datetime(from_datetime)
        to_datetime = ensure_datetime(to_datetime)

        validate_historical_data(resolution)

        payload = {"token": str(instrument.token),
                   "exchange": instrument.exchange if not indices else f"{instrument.exchange}::index",
                   "from": str(int(from_datetime.timestamp())) + '000',
                   "to": str(int(to_datetime.timestamp())) + '000',
                   "resolution": resolution
                   }

        getHistoricalResp = self._init_post(_endpoints_key="getHistoricalData", data=payload)

        if getHistoricalResp['stat'] == 'Not ok':
            return getHistoricalResp
        else:
            order_history = getHistoricalResp.get('result', [])
            df = pd.DataFrame(order_history)
            df = df.rename(columns={'time': 'datetime'})
            df = df[['datetime', 'open', 'high', 'low', 'close', 'volume']]
            print(df)
            return df.to_dict(orient="records")

        # # API call

    def get_WebsocketToken(self):
        # Get Order-Token from websocket token API to connect websocket
        Get_WStokenResp = self._init_get(_endpoints_key="getWSSession")
        return Get_WStokenResp

    def get_OrderFeed_websocket(self):

        # Fetch orderFeed from websocket via place order on market hours

        data = self.get_WebsocketToken()

        ws_token = data['result'][0]['orderToken']
        headers = {
            'Content-Type': 'application/json'
        }
        payload = {
            "orderToken": ws_token,
            "userId": self._user_id
        }
        session_data = json.dumps(payload)

        def on_message(ws, message):
            # Get message from websocket
            print(message)
            # Get Message received from websocket

        def on_error(ws, error):
            # Get error from websocket
            if type(ws) is not websocket.WebSocketApp:
                error = ws
            if self.__on_error:
                self.__on_error(error)

        def on_close(ws, close_status_code, close_msg):
            # WebSocket close connection
            self.ws_connection = False
            if self.__on_disconnect:
                self.__on_disconnect()
            print(f"WebSocket Closed. Status code: {close_status_code}, Reason: {close_msg}")

        def on_open(socket):
            # Websocket open connection
            socket.send(session_data)
            threading.Thread(target=heart_beat_connection, args=(socket,), daemon=True).start()

        def heart_beat_connection(ws):

            # WebSocket heartbeat connection to keep the API connected

            heartbeat_Flag = True
            while heartbeat_Flag:
                payload = {
                    "heartbeat": "h",
                    "userId": self._user_id
                }
                heartBeat_data = json.dumps(payload)
                ws.send(heartBeat_data)
                sleep(50)

        # Create the WebSocket connection
        ws = websocket.WebSocketApp(
            self._wss_url,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
            on_open=on_open,
            header=headers  # Pass headers if required
        )
        ws.run_forever()
        get_orderWebsocketResp = self._init_post(_endpoints_key="getWStoken", data=payload)
        return get_orderWebsocketResp

    def invalid_sess(self, session_ID):

        url = 'https://a3.aliceblueonline.com/open-api/od/v1/profile/invalidateWsSess'

        headers = {
            'Authorization': 'Bearer ' + session_ID,
            'Content-Type': 'application/json'
        }
        payload = {
            "source": "API",
            "userId": self._user_id
        }
        data = json.dumps(payload)
        response = requests.request("POST", url, headers=headers, data=data)
        return response.json()

    def createSession(self, session_ID):
        url = 'https://a3.aliceblueonline.com/open-api/od/v1/profile/createWsSess'

        headers = {
            'Authorization': 'Bearer ' + session_ID,
            'Content-Type': 'application/json'
        }
        payload = {
            "source": "API",
            "userId": self._user_id
        }
        data = json.dumps(payload)
        response = requests.request("POST", url, headers=headers, data=data)
        return response.json()

    def __ws_run_forever(self):
        while self.__stop_event.is_set() is False:
            try:
                self.ws.run_forever(ping_interval=3, ping_payload='{"t":"h"}', sslopt={"cert_reqs": ssl.CERT_NONE})
            except Exception as e:
                logger.warning(f"websocket run forever ended in exception, {e}")
            sleep(1)

    def on_message(self, ws, message):
        self.__subscribe_callback(message)
        data = json.loads(message)

    def on_error(self, ws, error):
        if (
                type(
                    ws) is not websocket.WebSocketApp):  # This workaround is to solve the websocket_client's compatiblity issue of older versions. ie.0.40.0 which is used in upstox. Now this will work in both 0.40.0 & newer version of websocket_client
            error = ws
        if self.__on_error:
            self.__on_error(error)

    def on_close(self, *arguments, **keywords):
        self.ws_connection = False
        if self.__on_disconnect:
            self.__on_disconnect()

    def stop_websocket(self):
        self.ws_connection = False
        self.ws.close()
        self.__stop_event.set()

    def on_open(self, ws):
        def sha256_encryption(val):
            return hashlib.sha256(val.encode("utf-8")).hexdigest()

        raw = self._session_id.strip()
        first = sha256_encryption(raw)
        enc_val = sha256_encryption(first)

        initCon = {
            "susertoken": enc_val,
            "t": "c",
            "actid": self._user_id + "_API",
            "uid": self._user_id + "_API",
            "source": "API"
        }

        self.ws.send(json.dumps(initCon))
        self.ws_connection = True
        if self.__on_open:
            self.__on_open()

    def subscribe(self, instrument):
        scripts = ""
        for __instrument in instrument:
            scripts = scripts + __instrument.exchange + "|" + str(__instrument.token) + "#"
        self.subscriptions = scripts[:-1]
        if self.market_depth:
            t = "d"  # Subscribe Depth
        else:
            t = "t"  # Subsribe token
        data = {
            "k": self.subscriptions,
            "t": t
        }
        # "m": "compact_marketdata"
        self.ws.send(json.dumps(data))

    def unsubscribe(self, instrument):
        global split_subscribes
        scripts = ""
        if self.subscriptions:
            split_subscribes = self.subscriptions.split('#')
        for __instrument in instrument:
            scripts = scripts + __instrument.exchange + "|" + str(__instrument.token) + "#"
            if self.subscriptions:
                split_subscribes.remove(__instrument.exchange + "|" + str(__instrument.token))
        self.subscriptions = split_subscribes

        if self.market_depth:
            t = "ud"
        else:
            t = "u"

        data = {
            "k": scripts[:-1],
            "t": t
        }
        self.ws.send(json.dumps(data))

    def start_websocket(self, socket_open_callback=None, socket_close_callback=None, socket_error_callback=None,
                        subscription_callback=None, check_subscription_callback=None, run_in_background=False,
                        market_depth=False):
        if check_subscription_callback != None:
            check_subscription_callback(self.script_subscription_instrument)
        session_request = self._session_id
        self.__on_open = socket_open_callback
        self.__on_disconnect = socket_close_callback
        self.__on_error = socket_error_callback
        self.__subscribe_callback = subscription_callback

        self.market_depth = market_depth
        if self.__stop_event != None and self.__stop_event.is_set():
            self.__stop_event.clear()
        if session_request:
            session_id = session_request

            first_hash = hashlib.sha256(session_id.encode('utf-8')).hexdigest()
            self.ENC = hashlib.sha256(first_hash.encode('utf-8')).hexdigest()

            invalidSess = self.invalid_sess(session_id)
            if invalidSess['status'] == 'Ok':
                print("STAGE 1: Invalidate the previous session :", invalidSess['status'])
                createSess = self.createSession(session_id)

                if createSess['status'] == 'Ok':
                    print("STAGE 2: Create the new session :", createSess['status'])
                    print("Connecting to Socket ...")
                    self.__stop_event = threading.Event()
                    websocket.enableTrace(False)
                    self.ws = websocket.WebSocketApp(self.websocket_url,
                                                     on_open=self.on_open,
                                                     on_message=self.on_message,
                                                     on_close=self.on_close,
                                                     on_error=self.on_error)

                    if run_in_background is True:
                        self.__ws_thread = threading.Thread(target=self.__ws_run_forever)
                        self.__ws_thread.daemon = True
                        self.__ws_thread.start()
                    else:
                        self.__ws_run_forever()

    """Web Socket Handler"""
