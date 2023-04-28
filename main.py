import asyncio
import dataclasses
import datetime as dt
import pathlib

import bfxapi
from omegaconf import OmegaConf
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from bfxapi import Client, FundingOffer

CONFIG_FOLDER = pathlib.Path('./conf.d')
CONFIG_FILE_PATH = CONFIG_FOLDER / 'config.yml'

config = OmegaConf.load(CONFIG_FILE_PATH)

# 設定 API 金鑰和密鑰
API_KEY = config.api_key
API_SECRET = config.api_secret

MAX_OFFER_AMOUNT = 1000
MIN_30D_RATE = 0.0006
MIN_7D_RATE = 0.00035
MIN_4D_6D_RATE = 0.00020
MIN_2D_3D_RATE = 0.00010


@dataclasses.dataclass
class FundingStrategy:
    f_type: str
    rate: float
    period: int

    def is_used_by(self, offer: FundingOffer):
        return (
                self.f_type == offer.f_type and
                self.rate == offer.rate and
                self.period == offer.period
        )


def main():
    client = Client(
        API_KEY=API_KEY,
        API_SECRET=API_SECRET,
    )

    scheduler = AsyncIOScheduler()
    scheduler.add_job(
        execute_funding_task,
        'interval',
        args=[client],
        seconds=10,
    )

    scheduler.add_job(
        show_stats,
        'interval',
        args=[client],
        hours=1,
    )

    scheduler.start()
    asyncio.get_event_loop().run_forever()


async def execute_funding_task(client: bfxapi.Client):
    strategy = await make_strategy(client)

    # 取消所有不同策略的訂單
    offers = await client.rest.get_funding_offers(symbol='fUSD')
    for offer in offers:
        if not strategy.is_used_by(offer):
            await client.rest.submit_cancel_funding_offer(offer.id)
            print(f'[{dt.datetime.now()}] 取消訂單 <{offer}>')

    # 如果錢包有錢但是小於 150，取消金額最小的訂單
    balance_available = await get_funding_balance(client)
    if 1 < balance_available < 150:
        offers = await client.rest.get_funding_offers(symbol='fUSD')

        min_amount_offer = None
        for offer in offers:
            if min_amount_offer is None or offer.amount < min_amount_offer.amount:
                min_amount_offer = offer

        if min_amount_offer:
            await client.rest.submit_cancel_funding_offer(min_amount_offer.id)
            print(f'[{dt.datetime.now()}] 取消訂單 <{min_amount_offer}>')

    # 根據當前餘額和策略下訂單
    balance_available = await get_funding_balance(client)
    while balance_available >= 150:
        amount = MAX_OFFER_AMOUNT
        if balance_available - MAX_OFFER_AMOUNT < 150:
            amount = balance_available

        resp = await client.rest.submit_funding_offer(
            symbol='fUSD',
            amount=amount,
            rate=strategy.rate,
            period=strategy.period,
            funding_type=strategy.f_type
        )
        print(f'[{dt.datetime.now()}] 新增訂單 {resp.notify_info} (金額：{amount})')
        balance_available -= amount


async def make_strategy(client: bfxapi.Client):
    strategy_func_list = [
        make_30d_strategy,
        make_7d_strategy,
        make_4d_to_6d_strategy,
        make_2d_to_3d_strategy,
    ]
    for strategy_func in strategy_func_list:
        strategy = await strategy_func(client)
        if strategy:
            return strategy


async def make_30d_strategy(client: bfxapi.Client):
    start = int((dt.datetime.now() - dt.timedelta(minutes=15)).timestamp() * 1000)

    rate = await get_highest_rate(client, 30, '5m', start=start)
    if rate < MIN_30D_RATE:
        return None

    return FundingStrategy(
        f_type=FundingOffer.Type.LIMIT,
        rate=rate,
        period=30,
    )


async def make_7d_strategy(client: bfxapi.Client):
    start = int((dt.datetime.now() - dt.timedelta(minutes=15)).timestamp() * 1000)

    rate = await get_highest_rate(client, 7, '5m', start=start)
    if rate < MIN_7D_RATE:
        return None

    return FundingStrategy(
        f_type=FundingOffer.Type.LIMIT,
        rate=rate,
        period=7,
    )


async def make_4d_to_6d_strategy(client: bfxapi.Client):
    highest_rate = -1
    period = None
    start = int((dt.datetime.now() - dt.timedelta(minutes=15)).timestamp() * 1000)

    for d in [4, 5, 6]:
        rate = await get_highest_rate(client, d, '5m', start=start)
        if rate > highest_rate:
            highest_rate = rate
            period = d

    if highest_rate < MIN_4D_6D_RATE:
        return None

    return FundingStrategy(
        f_type='LIMIT',
        rate=highest_rate,
        period=period,
    )


async def make_2d_to_3d_strategy(client: bfxapi.Client):
    highest_rate = -1
    period = None
    start = int((dt.datetime.now() - dt.timedelta(minutes=15)).timestamp() * 1000)

    for d in [2, 3]:
        rate = await get_highest_rate(client, d, '5m', start=start)
        if rate > highest_rate:
            highest_rate = rate
            period = d

    if highest_rate < MIN_2D_3D_RATE:
        highest_rate = MIN_2D_3D_RATE
        period = 2

    return FundingStrategy(
        f_type='LIMIT',
        rate=highest_rate,
        period=period,
    )


async def get_frr_rate(client: bfxapi.Client):
    [frr_rate, *_] = await client.rest.get_public_ticker('fUSD')
    return frr_rate


async def get_highest_rate(client: bfxapi.Client, period, timeframe, start=None, end=None):
    highest_rate = -1

    candles = await client.rest.get_public_candles(f'fUSD:p{period}', start=start, end=end, tf=timeframe)
    for candle in candles:
        [mts, open, close, high, low, volume] = candle
        if volume > 0:
            if high > highest_rate:
                highest_rate = high
    return highest_rate


async def get_funding_balance(client: bfxapi.Client):
    wallets = await client.rest.get_wallets()
    for wallet in wallets:
        if wallet.type == 'funding' and wallet.currency == 'USD':
            return wallet.balance_available


async def get_min_amount_offer(client: bfxapi.Client):
    offers = await client.rest.get_funding_offers(symbol='fUSD')

    min_amount_offer = None
    for offer in offers:
        if min_amount_offer is None or offer.amount < min_amount_offer.amount:
            min_amount_offer = offer
    return min_amount_offer


async def show_stats(client: bfxapi.Client):
    total_amount = 0
    total_earn = 0
    for credit in await client.rest.get_funding_credits(symbol='fUSD'):
        total_amount += credit.amount
        total_earn += credit.rate * credit.amount
    average_rate = total_earn / total_amount
    print(f'[{dt.datetime.now()}] 總借出: {total_amount} 每日收益: {total_earn} (平均利率: {average_rate * 100}%)')


if __name__ == '__main__':
    main()
