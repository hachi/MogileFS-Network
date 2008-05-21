#!/usr/bin/perl

use strict;
use warnings;
use Test::More tests => 4;
use FindBin qw($Bin);

use MogileFS::Network;

set(zone_one => '127.0.0.0/16');
set(zone_two => '10.0.0.0/8');
set(zone_three => '10.1.0.0/16');

is(lookup('127.0.0.1'), 'zone_one', "Standard match");
is(lookup('10.0.0.1'), 'zone_two', "Outer netblock match");
is(lookup('10.1.0.1'), 'zone_three', "Inner netblock match");
is(lookup('192.168.0.1'), undef, "Unknown zone");

sub lookup {
    return MogileFS::Network->zone_for_ip(@_);
}

sub set {
    MogileFS::Network->stuff_cache(@_);
}
