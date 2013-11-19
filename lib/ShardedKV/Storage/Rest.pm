package ShardedKV::Storage::Rest;

use Moose;
use Net::HTTP;

our $VERSION = 0.1;

with 'ShardedKV::Storage';

has 'url' => (
    is => 'ro',
    isa => 'Str',
    required => 1
);

has 'basepath' => (
    is => 'ro',
    isa => 'Str',
    required => 0,
    default => '',
    trigger => sub { 
        $_[1]
    }
);

sub _http {
    my $self = shift;
    my $s = $self->{_s};
    return $s if ($s && $s->connected);
    my ($host, $port) = split(':', $self->url);
    $self->{_s} = Net::HTTP->new(Host => $host, PeerPort => $port, KeepAlive => 1);
    return $self->{_s};
}

sub get {
    my ($self, $key) = @_;

    my $s = $self->_http;

    return unless $s;

    $s->write_request(GET => "/$key", 'User-Agent' => "p5-ShardedKV");
    my($code, $mess, %h) = $s->read_response_headers;
    my $value;
    if ($code >= 200 && $code < 300) {
        while (1) {
            my $buf;
            my $n = $s->read_entity_body($buf, 1024);
            last unless $n;
            $value .= $buf;
        }
    }
    return $value;
}

sub set {
    my ($self, $key, $value_ref) = @_;

    my $s = $self->_http;

    return 0 unless $s;

    $s->write_request(PUT => "/$key", 'User-Agent' => "p5-ShardedKV", $value_ref);
    my($code, $mess, %h) = $s->read_response_headers;
    if ($code >= 200 && $code < 300) {
        return 1;
    }
    return 0;
}

sub delete {
    my ($self, $key) = @_;

    my $s = $self->_http;

    return 0 unless $s;
    
    $s->write_request(DELETE => "/$key", 'User-Agent' => "p5-ShardedKV");
    my($code, $mess, %h) = $s->read_response_headers;
    if ($code >= 200 && $code < 300) {
        return 1;
    }
    return 0;
}

sub reset_connection {
    my $self = shift;
    delete $self->{_s};
    $self->_http; # force creation of a new connection
}

no Moose;

__PACKAGE__->meta->make_immutable;

__END__

=head1 NAME

ShardedKV::Storage::Rest - rest backend for ShardedKV

=head1 SYNOPSIS

  TODO

=head1 DESCRIPTION

A C<ShardedKV> storage backend that uses a remote http/rest storage.

Implements the C<ShardedKV::Storage> role.

=head1 SEE ALSO

L<ShardedKV>
L<ShardedKV::Storage>

=head1 AUTHORS

=over 4

=item * Andrea Guzzo <xant@cpan.org>

=back

=cut
