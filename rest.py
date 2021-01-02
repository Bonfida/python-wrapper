from typing import Optional, Dict, Any, List
from requests import Request, Session, Response


class BonfidaClient:
    _ENDPOINT = "https://serum-api.bonfida.com/"

    def __init__(self) -> None:
        self._session = Session()

    def _get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        return self._request('GET', path, params=params)

    def _request(self, method: str, path: str, **kwargs) -> Any:
        request = Request(method, self._ENDPOINT + path, **kwargs)
        response = self._session.send(request.prepare())
        return self._process_response(response)

    def _process_response(self, response: Response) -> Any:
        try:
            result = response.json()
        except ValueError:
            response.raise_for_status()
            raise
        else:
            if not result['success']:
                raise Exception(result['error'])
            return result['data']

    # Serum DEX

    def get_pairs(self) -> List[str]:
        return self._get("pairs")

    def get_recent_trades(self, market: str) -> List[dict]:
        # e.g market = "BTCUSDC"
        return self._get(f"trades/{market}")

    def get_all_recent_trades(self) -> List[dict]:
        return self._get("trades/all/recent")

    def get_volume(self, market: str) -> List[dict]:
        # e.g market = "BTCUSDC"
        return self._get(f"volumes/{market}")

    def get_orderbook(self, market: str) -> dict:
        # e.g market = "BTCUSDC"
        return self._get(f"orderbooks/{market}")

    def get_historical_prices(self, market: str, resolution: float,
                              start_time: float, end_time: float, limit: float) -> List[dict]:
        return self._get(f"candles/{market}?resolution={resolution}&startTime={start_time}&endTime={end_time}&limit={limit}")

    # Serum Swap

    def get_all_pools(self) -> List[dict]:
        return self._get("pools")

    def get_pool(self, mint_a: str, mint_b: str, start_time: float,
                 end_time: float, limit: float) -> List[dict]:
        return self._get(f"pools/{mint_a}/{mint_b}?startTime={start_time}&endTime={end_time}&limit={limit}")

    def get_pool_trade(self, symbol_source: str, symbol_destination: str, both_direction: bool) -> List[dict]:
        return self._get(f"pools/trades?symbolSource={symbol_source}&symbolDestination={symbol_destination}&bothDirections={both_direction}")

    def get_swap_volume(self) -> List[dict]:
        return self._get("pools/volumes/recent")

    def get_swap_historical_volume(self, mint_a: str, mint_b: str, start_time: float,
                                   end_time: float, limit: float) -> List[dict]:
        return self._get(f"pools/volumes?mintA={mint_a}&mintB={mint_b}&endTime={end_time}&startTime={start_time}&limit={limit}")

    def get_swap_historical_liquidity(self, mint_a: str, mint_b: str, start_time: float,
                                      end_time: float, limit: float) -> List[dict]:
        return self._get(f"pools/liquidity?mintA={mint_a}&mintB={mint_b}&endTime={end_time}&startTime={start_time}&limit={limit}")
