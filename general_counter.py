class GeneralCounterShardConfig(ndb.Model):
    """Tracks the number of shards for each named counter."""
    num_shards = ndb.IntegerProperty(default=20)

    @classmethod
    def all_keys(cls, name):
        """Returns all possible keys for the counter name given the config.

        Args:
            name: The name of the counter.

        Returns:
            The full list of ndb.Key values corresponding to all the possible
                counter shards that could exist.
        """
        config = cls.get_or_insert(name)
        shard_key_strings = [constants.SHARD_KEY_TEMPLATE.format(name, index)
                             for index in range(config.num_shards)]
        return [ndb.Key(GeneralCounterShard, shard_key_string)
                for shard_key_string in shard_key_strings]


class GeneralCounterShard(ndb.Model):
    """Shards for each named counter."""
    count = ndb.IntegerProperty(default=0)


    @classmethod
    def reset_count(self, name):
        all_keys = GeneralCounterShardConfig.all_keys(name)
        for counter in ndb.get_multi(all_keys):
            if counter is not None:
                counter.count = 0
                counter.put()


    @classmethod
    def get_count(self, name):
        """Retrieve the value for a given sharded counter.

        Args:
            name: The name of the counter.

        Returns:
            Integer; the cumulative count of all sharded counters for the given
                counter name.
        """
        total = memcache.get(name)
        if total is None:
            total = 0
            all_keys = GeneralCounterShardConfig.all_keys(name)
            for counter in ndb.get_multi(all_keys):
                if counter is not None:
                    total += counter.count
            memcache.add(name, total)
        return total


    @classmethod
    def increment(self, name, delta=1):
        """Increment the value for a given sharded counter.

        Args:
            name: The name of the counter.
        """
        config = GeneralCounterShardConfig.get_or_insert(name)
        GeneralCounterShard._increment(name, delta, config.num_shards)


    @classmethod
    @ndb.transactional
    def _increment(self, name, delta, num_shards):
        """Transactional helper to increment the value for a given sharded counter.

        Also takes a number of shards to determine which shard will be used.

        Args:
            name: The name of the counter.
            num_shards: How many shards to use.
        """
        index = random.randint(0, num_shards - 1)
        shard_key_string = constants.SHARD_KEY_TEMPLATE.format(name, index)
        counter = GeneralCounterShard.get_by_id(shard_key_string)
        if counter is None:
            counter = GeneralCounterShard(id=shard_key_string)
        counter.count += delta
        counter.put()
        # Memcache increment does nothing if the name is not a key in memcache
        if delta > 0:
            memcache.incr(name, delta=delta)
        else:
            memcache.decr(name, delta=-delta)


    @classmethod
    @ndb.transactional
    def increase_shards(self, name, num_shards):
        """Increase the number of shards for a given sharded counter.

        Will never decrease the number of shards.

        Args:
            name: The name of the counter.
            num_shards: How many shards to use.
        """
        config = GeneralCounterShardConfig.get_or_insert(name)
        if config.num_shards < num_shards:
            config.num_shards = num_shards
            config.put()
