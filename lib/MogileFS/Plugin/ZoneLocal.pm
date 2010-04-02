# ZoneLocal plugin for MogileFS, by hachi

package MogileFS::Plugin::ZoneLocal;

use strict;
use warnings;

our $VERSION = '0.01';
$VERSION = eval $VERSION;

use MogileFS::Worker::Query;
use MogileFS::Network;
use MogileFS::Util qw/error/;

sub prioritize_devs_current_zone;

sub load {
    my $local_network = MogileFS::Config->config('local_network');
    die "must define 'local_network' (ie: 10.5.0.0/16) in your mogilefsd.conf"
        unless $local_network;
    my $local_zone_test = MogileFS::Network->zone_for_ip($local_network);
    die "Could not resolve a local zone for $local_network. Please ensure this IP is within a configured zone"
        unless $local_zone_test;

    MogileFS::register_global_hook( 'cmd_get_paths_order_devices', sub {
        my $devices = shift;
        my $sorted_devs = shift;

        @$sorted_devs = prioritize_devs_current_zone(
                        $MogileFS::REQ_client_ip,
                        MogileFS::Worker::Query::sort_devs_by_utilization(@$devices)
                        );

        return 1;
    });

    MogileFS::register_global_hook( 'cmd_create_open_order_devices', sub {
        my $devices = shift;
        my $sorted_devs = shift;

        @$sorted_devs = prioritize_devs_current_zone(
                        $MogileFS::REQ_client_ip,
                        MogileFS::Worker::Query::sort_devs_by_freespace(@$devices)
                        );

        return 1;
    });

    MogileFS::register_global_hook( 'replicate_order_final_choices', sub {
        my $devs    = shift;
        my $choices = shift;

        my @sorted = prioritize_devs_current_zone(
                     MogileFS::Config->config('local_network'),
                     map { $devs->{$_} } @$choices);
        @$choices  = map { $_->id } @sorted;

        return 1;
    });

    return 1;
}

sub unload {
    # remove our hooks
    MogileFS::unregister_global_hook( 'cmd_get_paths_order_devices' );
    MogileFS::unregister_global_hook( 'cmd_create_open_order_devices' );
    MogileFS::unregister_global_hook( 'replicate_order_final_choices' );

    return 1;
}

sub prioritize_devs_current_zone {
    my $local_ip = shift;
    my $current_zone = MogileFS::Network->zone_for_ip($local_ip);
    error("Cannot find current zone for local ip $local_ip")
        unless defined $current_zone;

    my (@this_zone, @other_zone);

    foreach my $dev (@_) {
        my $ip = $dev->host->ip;
        my $host_id = $dev->host->id;
        my $zone = MogileFS::Network->zone_for_ip($ip);
        error("Cannot find zone for remote IP $ip")
            unless defined $zone;

        if ($current_zone eq $zone) {
            push @this_zone, $dev;
        } else {
            push @other_zone, $dev;
        }
    }

    return @this_zone, @other_zone;
}

1;
