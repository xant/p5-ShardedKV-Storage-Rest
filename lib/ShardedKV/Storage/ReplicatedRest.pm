package ShardedKV::Storage::ReplicatedRest;

use ShardedKV::Storage::Rest;

use strict;
use warnings;

use Moose;

with 'ShardedKV::Storage';

has 'urls' => (
    is => 'ro',
    isa => 'ArrayRef[Str]',
    required => 1
);

has 'endpoints' => (
    is => 'ro',
    isa => 'ArrayRef[ShardedKV::Storage::Rest]',
    required => 0,
    lazy    => 1,
    builder => '_build_endpoints'
);

has 'max_failures' => (
    is => 'ro',
    isa => 'Int',
    required => 0,
    default => 0
);


sub _build_endpoints {
    my $self = shift;
    $self->{endpoints} = [];
    push @{$self->{endpoints}}, ShardedKV::Storage::Rest->new(url => $_) for @{$self->urls};
    return $self->{endpoints};
}

sub get {
    my ($self, $key) = @_;

    for my $endpoint (@{$self->endpoints}) {
        my $data = $endpoint->get($key);
        return $data if defined($data);
    }

    return undef;
}

sub set {
    my ($self, $key, $value) = @_;

    my $failures = 0;

    for my $endpoint (@{$self->endpoints}) {
        my $r;
        eval {
            $r = $endpoint->set($key, $value);
            1;
        } or do {
            # warn -- error setting key=$key
            $r = 0;
        };
        if (!$r) {
            $failures++;
        }
    }

    return ($failures > $self->max_failures) ? 0 : 1;
}

sub delete {
    my ($self, $key) = @_;


    my $failures = 0;

    for my $endpoint (@{$self->endpoints}) {
        my $r;
        eval {
            $r = $endpoint->delete($key);
            1;
        } or do {
            # warn -- error deleting key=$key
            $r = 0;
        };
        if (!$r) {
            $failures++;
        }
    }

    return ($failures > $self->max_failures) ? 0 : 1;
}

sub reset_connection {
    my ($self, $key) = @_;

    my $failures = 0;

    for my $endpoint (@{$self->endpoints}) {
        my $r;
        eval {
            $r = $endpoint->reset_connection($key);
            1;
        } or do {
            $r = 0;
            # warn -- error resetting connection for key=$key
        };
        if (!$r) {
            $failures++;
        }
    }
    return ($failures > $self->max_failures) ? 0 : 1;
}

1;
