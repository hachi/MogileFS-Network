package MogileFS::Network;

use strict;
use warnings;

use Net::Netmask;
use Net::Patricia;
use MogileFS::Config;

our $VERSION = "0.01";

use constant DEFAULT_RELOAD_INTERVAL => 60;

my $trie = Net::Patricia->new(); # Net::Patricia object used for cache and lookup.
my $next_reload = 0;             # Epoch time at or after which the trie expires and must be regenerated.

sub zone_for_ip {
    my $class = shift;
    my $ip = shift;

    return unless $ip;

    check_cache();

    return $trie->match_string($ip);
}

sub check_cache {
    # Reload the trie if it's expired
    return unless (time() >= $next_reload);

    $trie = Net::Patricia->new();

    my @zones = split(/\s*,\s*/,MogileFS::Config->server_setting("network_zones"));

    my @netmasks; # [ $bits, $netmask, $zone ], ...

    foreach my $zone (@zones) {
        my $zone_masks = MogileFS::Config->server_setting("zone_$zone");

        foreach my $network_string (split /[,\s]+/, $zone_masks) {
            if (not $network_string) {
                warn "couldn't find network_zone <<zone_$zone>> check your server settings";
                next;
            }

            #if ($cache{$zone}) {
            #    warn "duplicate netmask <$netmask> in network zones. check your server settings";
            #}

            #$cache{$zone} = Net::Netmask->new2($netmask);
            my $netmask = Net::Netmask->new2($network_string);

            if (Net::Netmask::errstr()) {
                warn "couldn't parse <$zone> as a netmask. error was <" . Net::Netmask::errstr().
                     ">. check your server settings";
                next;
            }

            push @netmasks, [$netmask->bits, $netmask, $zone];
        }
    }

    foreach my $set (sort { $a->[0] <=> $b->[0] } @netmasks) {
        my ($bits, $netmask, $zone) = @$set;

        $trie->add_string("$netmask", $zone);
    }

    my $interval = MogileFS::Config->server_setting("network_reload_interval")
                   || DEFAULT_RELOAD_INTERVAL;

    clear_and_build_cache();

    $next_reload = time() + $interval;

    return 1;
}

sub stuff_cache { # for testing, or it'll try the db
    my ($self, $zone, $netmask) = @_;

    $trie->add_string("$netmask", $zone);
    $next_reload = time() + 120; # If the test takes more than two minutes we're gonna break
}

1;
