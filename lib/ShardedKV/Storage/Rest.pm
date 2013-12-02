package ShardedKV::Storage::Rest;

use strict;
use Moose;
use Net::HTTP;
use URI;
use Socket;

our $VERSION = '0.3';

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
    default => sub { my $self = shift;
                     my $uri = URI->new($self->url);
                     return $uri->path },
);

sub _http {
    my $self = shift;
    my $s = $self->{_s};
    if ($s && $s->connected) {
        my $buff;
        eval {
            my $ret = recv($s, $buff, 1, MSG_PEEK | MSG_DONTWAIT);
            return $s
                if defined($ret);
        }
    }
    my $uri = URI->new($self->url);
    $self->{_s} = Net::HTTP->new(Host => $uri->host, PeerPort => $uri->port, KeepAlive => 1);
    return $self->{_s};
}

sub _send_http_request {
    my ($self, $type, $key, $body) = @_;

    my $s = $self->_http;

    return unless $s;

    my $uri = URI->new($self->url);

    my ($code, $mess, %h);
    eval {
        $s->write_request($type => $uri->path . "/$key", 'User-Agent' => "p5-ShardedKV", $body ? $body : "");
        ($code, $mess, %h) = $s->read_response_headers;
        1;
    } or do {
        warn $@;
        return;
    };
    return ($s, $code, $mess, %h); 
}

sub get {
    my ($self, $key) = @_;

    my ($s, $code, $mess, %h) = $self->_send_http_request('GET', $key);
    return unless defined $code;

    my $value;
    if ($code >= 200 && $code < 300) {
        my $content_length = $h{'Content-Length'};
        my $to_read = $content_length;
        return unless $to_read;
        my $len = 0;
        while ($len != $to_read) {
            my $buf;
            my $n;
            eval { 
                $n = $s->read_entity_body($buf, $to_read);
            } or do {
                last;
            };
            if ($n > 0) {
                $len += $n;
                $to_read -= $n;
                $value .= $buf;
            }
        }
    }
    return $value;
}

sub set {
    my ($self, $key, $value_ref) = @_;

    return unless $value_ref;

    my ($s, $code, $mess, %h) = $self->_send_http_request('PUT', $key, $value_ref);
    return unless defined $code;

    if ($code >= 200 && $code < 300) {
        return 1;
    }
    return 0;
}

sub delete {
    my ($self, $key) = @_;

    my ($s, $code, $mess, %h) = $self->_send_http_request('DELETE', $key);
    return unless defined $code;

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

  use ShardedKV;
  use ShardedKV::Storage::Rest;
  ... create ShardedKV...
  my $storage = ShardedKV::Storage::Rest->new(
    url => 'http://localhost:679',
  );
  ... put storage into ShardedKV...
  
  # values are scalar references to strings
  $skv->set("foo", 'bar');
  my $value_ref = $skv->get("foo");


=head1 DESCRIPTION

A C<ShardedKV> storage backend that uses a remote http/rest storage.

Implements the C<ShardedKV::Storage> role.

=head1 PUBLIC ATTRIBUTES

=over 4

=head2 url

A 'http://hostname:port[/basepath]' url string pointing at the http/rest server for this shard.
Required.

=head2 basepath

The base path part of the url provided at initialization time
Read Only

=back

=head1 SEE ALSO

L<ShardedKV>
L<ShardedKV::Storage>

=head1 AUTHORS

=over 4

=item Andrea Guzzo <xant@cpan.org>

=back

=cut
