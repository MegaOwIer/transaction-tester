#include "Transaction.h"

#include <cassert>
#include <cstddef>
#include <functional>
#include <shared_mutex>
#include <atomic>

static const time_t TRY_PERIORD = 50;
static const int MAX_TRY_TIMES = 5;

enum STATUS_CODE {
    ROLLBACK, RETRY_LATER, SUCCESS
};

class RecordItem {
public:
    void initialize(const RecordData &_data) {
        data = _data;
    }

    STATUS_CODE readData(const timestamp_t &trans_time, RecordData &result) {
        std::unique_lock<std::shared_timed_mutex> lock(lockHoldersLock, std::defer_lock);
        if (!lock.try_lock_for(std::chrono::nanoseconds(TRY_PERIORD))) {
            return RETRY_LATER;
        }

        // already hold lock
        if (lockHolders.count(trans_time)) {
            result = data;
            return SUCCESS;
        }

        // R-W conflict
        if (lockType) {
            // assert(lockHolders.size() == 1);
            if (*lockHolders.begin() < trans_time) {
                return ROLLBACK;
            } else {
                return RETRY_LATER;
            }
        }

        // no conflict
        result = data;
        lockHolders.insert(trans_time);
        return SUCCESS;
    }

    std::pair<STATUS_CODE, RecordData> writeData(const timestamp_t &trans_time, const RecordData &newData) {
        std::unique_lock<std::shared_timed_mutex> lock(lockHoldersLock, std::defer_lock);
        if (!lock.try_lock_for(std::chrono::nanoseconds(TRY_PERIORD))) {
            return {RETRY_LATER, RecordData()};
        }

        // no conflict
        if (checkXlock(trans_time)) {
            lockType = true;
            lockHolders.insert(trans_time);
            auto ret = std::make_pair(SUCCESS, std::move(data));
            data = newData;
            return ret;
        }

        // W-W/W-R conflict
        for (const timestamp_t &cur : lockHolders) {
            if (cur < trans_time) {
                return {ROLLBACK, RecordData()};
            }
        }
        return {RETRY_LATER, RecordData()};
    }

    void unlockData(const timestamp_t &trans_time) {
        std::unique_lock<std::shared_timed_mutex> lock(lockHoldersLock);
        // assert(lockHolders.count(trans_time));
        lockHolders.erase(trans_time);
        lockType = false;
    }

private:
    RecordData data;

    bool lockType;  // true for exclusive, false for shared
    std::unordered_set<timestamp_t> lockHolders;
    std::shared_timed_mutex lockHoldersLock;

    bool checkXlock(const timestamp_t &trans_time) {
        return lockHolders.empty() || 
               (lockHolders.size() == 1 && lockHolders.count(trans_time));
    }
};

static std::unordered_map<RecordKey, RecordItem> storage;

static std::vector<transaction_id_t> serializationOrder;
static std::mutex serializationOrderLock;

void preloadData(const std::unordered_map<RecordKey, RecordData> &initialRecords) {
    for (auto &[key, data] : initialRecords) {
        storage[key].initialize(data);
    }
}

std::vector<transaction_id_t> getSerializationOrder() {
    return serializationOrder;
}

void Transaction::start() {
    timestamp = getTimestamp();
}

bool Transaction::read(const RecordKey &key, RecordData &result) {
    for (int i = 0; i < MAX_TRY_TIMES; i++) {
        STATUS_CODE ret = storage[key].readData(timestamp, result);
        switch (ret) {
            case ROLLBACK: return false;
            case RETRY_LATER: continue;
            case SUCCESS: {
                access_history.insert(key);
                return true;
            }
        }
    }
    return false;
}

bool Transaction::write(const RecordKey &key, const RecordData &newData) {
    for (int i = 0; i < MAX_TRY_TIMES; i++) {
        auto ret = storage[key].writeData(timestamp, newData);
        switch (ret.first) {
            case ROLLBACK: return false;
            case RETRY_LATER: continue;
            case SUCCESS: {
                access_history.insert(key);
                write_log.emplace_back(key, ret.second);
                return true;
            }
        }
    }
    return false;
}

bool Transaction::commit() {
    {
        std::lock_guard lock(serializationOrderLock);
        serializationOrder.push_back(id);
    }
    for (const RecordKey &cur : access_history) {
        storage[cur].unlockData(timestamp);
    }
    return true;
}

void Transaction::rollback() {
    std::for_each(write_log.rbegin(), write_log.rend(), [&] (const std::pair<RecordKey, RecordData> &cur) {
        auto ret = storage[cur.first].writeData(timestamp, cur.second);
        // assert(ret.first == SUCCESS);
    });
    for (const RecordKey &cur : access_history) {
        storage[cur].unlockData(timestamp);
    }
}
