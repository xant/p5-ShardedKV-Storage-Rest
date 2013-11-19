#!/usr/bin/perl

use strict;
use Test::More;

use ShardedKV;
use ShardedKV::Continuum::Ketama;

BEGIN { use_ok( 'ShardedKV::Storage::Rest' ); }

my $continuum_spec = [
    ["shard1", 100], # shard name, weight
    ["shard2", 150],
];
my $continuum = ShardedKV::Continuum::Ketama->new(from => $continuum_spec);

# Redis storage chosen here, but can also be "Memory" or "MySQL".
# "Memory" is for testing. Mixing storages likely has weird side effects.
my %storages = (
    shard1 => ShardedKV::Storage::Rest->new(
        url => 'localhost:6379',
    ),
    shard2 => ShardedKV::Storage::Rest->new(
        url => 'localhost:6380',
    ),
);

my $skv = ShardedKV->new(
    storages => \%storages,
    continuum => $continuum,
);

my $key = "test_key";
my $value = "test_value";

my $num_items = 10;
foreach my $i (0..$num_items) {
    is ($skv->set("${key}$i", "${value}$i"), 1);
}
foreach my $i (0..$num_items) {
    my $stored_value = $skv->get("${key}$i");
    is($stored_value, "${value}$i");
    $skv->delete("${key}$i");
    is($skv->get("${key}$i"), undef);
}

done_testing();
