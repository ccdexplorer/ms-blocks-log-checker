from ccdexplorer_fundamentals.enums import NET
from ccdexplorer_fundamentals.mongodb import (
    Collections,
)
from pymongo import ReplaceOne
from pymongo.collection import Collection
from rich.console import Console

from .utils import Utils as _utils

console = Console()


class BlocksLogChecker(_utils):

    async def get_block_log_height_with_logged_events(
        self, db_to_use: dict[Collections, Collection], skip: int, limit: int
    ):
        pipeline = [
            {"$match": {"tokens_logged_events": {"$exists": True}}},
            {"$sort": {"_id": 1}},
            {"$skip": skip},
            {"$limit": limit},
            {"$project": {"_id": 1}},
        ]
        all_heights_in_db = [
            x["_id"]
            for x in await db_to_use[Collections.blocks_log]
            .aggregate(pipeline)
            .to_list(length=None)
        ]
        return all_heights_in_db

    async def get_block_heights_in_range(
        self, db_to_use: dict[Collections, Collection], start: int, stop: int
    ):
        pipeline = [
            {"$match": {"height": {"$gte": start}}},
            {"$match": {"height": {"$lt": stop}}},
            {"$sort": {"height": 1}},
            {"$project": {"height": 1}},
        ]
        all_heights_in_db = [
            x["height"]
            for x in await db_to_use[Collections.blocks]
            .aggregate(pipeline)
            .to_list(length=None)
        ]
        return all_heights_in_db

    async def get_block_logs_heights_in_range(
        self, db_to_use: dict[Collections, Collection], start: int, stop: int
    ):
        pipeline = [
            {"$match": {"_id": {"$gte": start}}},
            {"$match": {"_id": {"$lt": stop}}},
            {"$sort": {"_id": 1}},
            {"$project": {"_id": 1}},
        ]
        all_heights_in_db = [
            x["_id"]
            for x in await db_to_use[Collections.blocks_log]
            .aggregate(pipeline)
            .to_list(length=None)
        ]
        return all_heights_in_db

    async def get_individual_content_in_range(
        self,
        db_to_use: dict[Collections, Collection],
        start: int,
        stop: int,
        collection: Collections,
    ):
        additional_filter = {"$match": {"_id": {"$exists": True}}}
        if collection == Collections.transactions:
            param = "block_info.height"
        if collection == Collections.impacted_addresses:
            param = "block_height"
            additional_filter = {"$match": {"effect_type": {"$ne": "Account Reward"}}}
        if collection == Collections.tokens_logged_events:
            param = "block_height"
        if collection == Collections.involved_accounts_transfer:
            param = "block_height"
        if collection == Collections.special_events:
            param = "_id"

        pipeline = [
            {"$match": {param: start}},
            additional_filter,
            # {"$match": {param: {"$lt": stop}}},
            {"$project": {"_id": 1}},
        ]
        all_ids_in_range = [
            x["_id"]
            for x in await db_to_use[collection]
            .aggregate(pipeline)
            .to_list(length=None)
        ]
        return sorted(all_ids_in_range)

    async def ids_found_in_content(
        self,
        db_to_use: dict[Collections, Collection],
        ids_list: list,
        collection: Collections,
    ):
        if len(ids_list) == 0:
            return 0

        pipeline = [{"$match": {"_id": {"$in": ids_list}}}, {"$count": "ids_found"}]
        the_list = await db_to_use[collection].aggregate(pipeline).to_list(length=None)
        if len(the_list) > 0:
            ids_found = the_list[0]["ids_found"]
        else:
            ids_found = 0
        return ids_found  # == len(ids_list)

    async def get_block_logs_content_in_range(
        self,
        db_to_use: dict[Collections, Collection],
        start: int,
        stop: int,
        property: str,
    ):
        pipeline = [
            {"$match": {"_id": start}},
            # {"$unwind": f"${property}"},
            # {"$group": {"_id": None, f"all_{property}": {"$push": f"${property}"}}},
            {"$project": {"_id": 0, f"{property}": 1}},
        ]

        all_content_in_db = (
            await db_to_use[Collections.blocks_log]
            .aggregate(pipeline)
            .to_list(length=None)
        )
        the_list = []
        if len(all_content_in_db) > 0:
            if f"{property}" in all_content_in_db[0]:
                the_list = all_content_in_db[0][f"{property}"]

        return sorted(the_list)

    async def check_range(
        self, db_to_use: dict[Collections, Collection], start: int, stop: int
    ):
        block_ok = True
        blocks_in_range = await self.get_block_heights_in_range(db_to_use, start, stop)
        blocks_log_in_range = await self.get_block_logs_heights_in_range(
            db_to_use, start, stop
        )
        log = {}
        if blocks_in_range != blocks_log_in_range:
            log[start] = [] if start not in log else log[start]
            log[start].append("blocks_in_range != blocks_log_in_range")

        # now go into blocks_log to find content we SHOULD have saved
        ## Tranactions
        txs_should_be_in_range = await self.get_block_logs_content_in_range(
            db_to_use, start, stop, "transaction_hashes"
        )
        txs_in_range = await self.get_individual_content_in_range(
            db_to_use, start, stop, Collections.transactions
        )

        if txs_should_be_in_range != txs_in_range:
            log[start] = [] if start not in log else log[start]
            log[start].append(
                f"transaction_hashes: missing {len(txs_should_be_in_range) - len(txs_in_range)}"
            )
            block_ok = False
        ## Tranactions

        ## Impacted Addresses
        ia_should_be_in_range = await self.get_block_logs_content_in_range(
            db_to_use, start, stop, "impacted_addresses"
        )
        ia_in_range = await self.get_individual_content_in_range(
            db_to_use, start, stop, Collections.impacted_addresses
        )

        if ia_should_be_in_range != ia_in_range:
            log[start] = [] if start not in log else log[start]
            log[start].append(
                f"impacted_addresses: missing {len(ia_should_be_in_range) - len(ia_in_range)}"
            )
            block_ok = False
        ## Impacted Addresses

        ## Tokens Logged Events
        logged_events_should_be_in_range = await self.get_block_logs_content_in_range(
            db_to_use, start, stop, "tokens_logged_events"
        )
        logged_events_in_range = await self.get_individual_content_in_range(
            db_to_use, start, stop, Collections.tokens_logged_events
        )

        if logged_events_should_be_in_range != logged_events_in_range:
            log[start] = [] if start not in log else log[start]
            log[start].append(
                f"tokens_logged_events: missing {len(logged_events_should_be_in_range) - len(logged_events_in_range)}"
            )
            block_ok = False
        ## Tokens Logged Events

        ## Involved Accounts Transfer
        ia_transfers_should_be_in_range = await self.get_block_logs_content_in_range(
            db_to_use, start, stop, "involved_accounts_transfer"
        )
        ia_transfers_in_range = await self.get_individual_content_in_range(
            db_to_use, start, stop, Collections.involved_accounts_transfer
        )

        if ia_transfers_should_be_in_range != ia_transfers_in_range:
            log[start] = [] if start not in log else log[start]
            log[start].append(
                f"involved_accounts_transfer: missing {len(ia_transfers_should_be_in_range) - len(ia_transfers_in_range)}"
            )
            block_ok = False
        ## Involved Accounts Transfer

        ## Special Events
        special_events_should_be_in_range = [start]
        special_events_in_range = await self.get_individual_content_in_range(
            db_to_use, start, stop, Collections.special_events
        )

        if special_events_should_be_in_range != special_events_in_range:
            log[start] = [] if start not in log else log[start]
            log[start].append(
                f"special_events: missing {len(special_events_should_be_in_range) - len(special_events_in_range)}"
            )
            block_ok = False
        ## Involved Accounts Transfer

        return block_ok, log

    async def lookup_block_at_net(self, net: NET, block_height: int):

        console.log(f"Running lookup_block_at_net for block {block_height} at {net}")
        db: dict[Collections, Collection] = (
            self.motor_mainnet if net == NET.MAINNET else self.motor_testnet
        )
        block_ok, log = await self.check_range(db, block_height, block_height + 1)
        if not block_ok:
            print(f"Repairing block {block_height:,.0f}. Reasons: {log[block_height]}")
            tooter_message = f"{net.value}: Repairing block {block_height:,.0f}. Reasons: {log[block_height]}"
            self.send_to_tooter(tooter_message)

            current_result = await db[Collections.helpers].find_one(
                {"_id": "special_purpose_block_request"}
            )
            current_list: list = current_result["heights"]
            current_list.append(block_height)
            current_list = list(set(current_list))

            d = {
                "_id": "special_purpose_block_request",
                "heights": current_list,
            }
            _ = await db[Collections.helpers].bulk_write(
                [
                    ReplaceOne(
                        {"_id": "special_purpose_block_request"},
                        replacement=d,
                        upsert=True,
                    )
                ]
            )

    async def cleanup(self):

        for net in [NET.MAINNET]:
            console.log(f"Running cleanup for {net}")
            db: dict[Collections, Collection] = (
                self.motor_mainnet if net == NET.MAINNET else self.motor_testnet
            )

            stop = await db[Collections.helpers].find_one(
                {"_id": "heartbeat_last_processed_block"}
            )
            stop = stop["height"]
            start = stop - 10000
            console.log(f"Running cleanup for {net} for {start:,.0f}-{stop:,.0f}")
            block_ok, log = await self.check_range(db, start, stop)
            if not block_ok:
                print(f"{start}-{stop} ERROR")

                for micro_step in range(start, stop + 1):
                    block_ok, log = await self.check_range(
                        db, micro_step, micro_step + 1
                    )
                    if not block_ok:
                        print(
                            f"Repairing block {micro_step:,.0f}. Reasons: {log[micro_step]}"
                        )
                        tooter_message = f"{net.value}: Repairing block {micro_step:,.0f}. Reasons: {log[micro_step]}"
                        self.send_to_tooter(tooter_message)

                        current_result = await db[Collections.helpers].find_one(
                            {"_id": "special_purpose_block_request"}
                        )
                        current_list: list = current_result["heights"]
                        current_list.append(micro_step)
                        current_list = list(set(current_list))

                        d = {
                            "_id": "special_purpose_block_request",
                            "heights": current_list,
                        }
                        _ = await db[Collections.helpers].bulk_write(
                            [
                                ReplaceOne(
                                    {"_id": "special_purpose_block_request"},
                                    replacement=d,
                                    upsert=True,
                                )
                            ]
                        )
            console.log(
                f"Running cleanup for {net} for {start:,.0f}-{stop:,.0f}. Done."
            )
