#!/usr/bin/perl

use strict;
use Test::More;

use ShardedKV;
use ShardedKV::Continuum::Ketama;

BEGIN { use_ok( 'ShardedKV::Storage::Rest' ); }

my @test_urls = split(';', $ENV{TEST_URLS});
unless(@test_urls) {
    warn "no TEST_URLS environment variable set, skipping tests";
    done_testing();
    exit(0);
}



my $continuum_spec;
foreach my $i (1..@test_urls) {
    push(@$continuum_spec, ["shard$i", 100]);
}
my $continuum = ShardedKV::Continuum::Ketama->new(from => $continuum_spec);

# Redis storage chosen here, but can also be "Memory" or "MySQL".
# "Memory" is for testing. Mixing storages likely has weird side effects.
my %storages;
my $cnt = 0;
foreach my $test_url (@test_urls) {
    $storages{"shard".++$cnt} = ShardedKV::Storage::Rest->new(url => $test_url);
}

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
