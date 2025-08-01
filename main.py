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

BITFINEX_FEES = 0.15

MAX_OFFER_AMOUNT = 1000
MIN_RATE = 0.0001
MIN_RATE_INCR_PER_DAY = 0.00003
POSSIBLE_PERIOD = (2, 3, 4, 5, 6, 7, 8, 10, 14, 15, 16, 20, 21, 22, 24, 30)


def get_annual_rate(rate, period):
    return (1 + rate * period) ** (365 / period) - 1


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
        minutes=1,
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
        if offer.f_type == FundingOffer.Type.FRR_DELTA:
            continue

        if not strategy.is_used_by(offer):
            await client.rest.submit_cancel_funding_offer(offer.id)
            print(f'[{dt.datetime.now()}] 取消訂單 {offer}')

    # 如果錢包有錢但是小於 150，取消金額最小的訂單
    balance_available = await get_funding_balance(client)
    if 1 < balance_available < 150:
        offers = await client.rest.get_funding_offers(symbol='fUSD')

        min_amount_offer = None
        for offer in offers:
            if offer.f_type == FundingOffer.Type.FRR_DELTA:
                continue

            if min_amount_offer is None or offer.amount < min_amount_offer.amount:
                min_amount_offer = offer

        if min_amount_offer:
            await client.rest.submit_cancel_funding_offer(min_amount_offer.id)
            print(f'[{dt.datetime.now()}] 取消訂單 {min_amount_offer}')

    # 如果沒有 FRR 訂單，就下一個
    balance_available = await get_funding_balance(client)
    if balance_available >= 150 and not await has_frr_offer(client):
        amount = balance_available
        if amount > MAX_OFFER_AMOUNT:
            amount = MAX_OFFER_AMOUNT

        resp = await client.rest.submit_funding_offer(
            symbol='fUSD',
            amount=amount,
            rate=0,
            period=30,
            funding_type=FundingOffer.Type.FRR_DELTA
        )
        print(f'[{dt.datetime.now()}] 新增 FRR 訂單 {resp.notify_info})')

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
    start = int((dt.datetime.now() - dt.timedelta(hours=1)).timestamp() * 1000)
    possible_rates = []

    # 找出最小可接受利率
    min_rate = await get_highest_rate(client, 2, '5m', start=start)
    if not min_rate or min_rate < MIN_RATE:
        min_rate = MIN_RATE

    for period in POSSIBLE_PERIOD:
        rate = await get_highest_rate(client, period, '5m', start=start)
        if rate and rate >= min_rate + (period - 2) * MIN_RATE_INCR_PER_DAY:
            possible_rates.append((get_annual_rate(rate, period), period, rate))

    if not possible_rates:
        # print('沒找到最佳利率，掛最低利率')
        return FundingStrategy(
            f_type=FundingOffer.Type.LIMIT,
            rate=min_rate,
            period=2,
        )

    possible_rates.sort(reverse=True)
    _, acceptable_period, acceptable_rate = possible_rates[0]

    # print('從下面可選利率選出第一個為最佳利率')
    # for annual_rate, period, rate in possible_rates:
    #     print(f'週期: {period} 利率: {rate} (計算年利率: {annual_rate})')

    return FundingStrategy(
        f_type=FundingOffer.Type.LIMIT,
        rate=acceptable_rate,
        period=acceptable_period,
    )


async def get_frr_rate(client: bfxapi.Client):
    [frr_rate, *_] = await client.rest.get_public_ticker('fUSD')
    return frr_rate


async def get_highest_rate(client: bfxapi.Client, period, timeframe, start=None, end=None):
    highest_rate = None

    candles = await client.rest.get_public_candles(f'fUSD:p{period}', start=start, end=end, tf=timeframe)
    for candle in candles:
        [mts, open, close, high, low, volume] = candle
        if volume > 0:
            if highest_rate is None or high > highest_rate:
                highest_rate = high
    return highest_rate


async def get_funding_balance(client: bfxapi.Client):
    wallets = await client.rest.get_wallets()
    for wallet in wallets:
        if wallet.type == 'funding' and wallet.currency == 'USD':
            return wallet.balance_available


async def has_frr_offer(client: bfxapi.Client):
    offers = await client.rest.get_funding_offers(symbol='fUSD')
    for offer in offers:
        if offer.f_type == FundingOffer.Type.FRR_DELTA:
            return True
    return False


async def get_total_asset(client: bfxapi.Client):
    wallets = await client.rest.get_wallets()
    for wallet in wallets:
        if wallet.type == 'funding' and wallet.currency == 'USD':
            return wallet.balance


async def get_min_amount_offer(client: bfxapi.Client):
    offers = await client.rest.get_funding_offers(symbol='fUSD')

    min_amount_offer = None
    for offer in offers:
        if min_amount_offer is None or offer.amount < min_amount_offer.amount:
            min_amount_offer = offer
    return min_amount_offer


async def show_stats(client: bfxapi.Client):
    frr_rate = await get_frr_rate(client=client)

    total_amount = 0
    total_earn = 0
    for credit in await client.rest.get_funding_credits(symbol='fUSD'):
        rate = credit.rate

        # 如果 rate = 0 就當 FRR
        if rate == 0:
            rate = frr_rate

        total_amount += credit.amount
        total_earn += rate * credit.amount

    average_rate = total_earn / total_amount
    final_earn = total_earn * (1 - BITFINEX_FEES)
    print(f'[{dt.datetime.now()}] 總借出: {total_amount} 每日收益: {final_earn} (平均利率: {average_rate})')


if __name__ == '__main__':
    main()
