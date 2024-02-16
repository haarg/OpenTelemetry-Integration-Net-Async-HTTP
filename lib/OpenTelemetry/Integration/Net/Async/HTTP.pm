package OpenTelemetry::Integration::Net::Async::HTTP;
use strict;
use warnings;

our $VERSION = '0.001000';
$VERSION =~ tr/_//d;

package OpenTelemetry::Integration::Net::Async::HTTP 0.001001;
use v5.32;
use experimental 'signatures';

use Hash::Util::FieldHash qw(fieldhash);
use List::Util qw(none);
use OpenTelemetry;
use OpenTelemetry::Constants qw( SPAN_KIND_CLIENT SPAN_STATUS_ERROR SPAN_STATUS_OK );
use Time::HiRes ();
use Socket qw(IPPROTO_TCP IPPROTO_UDP PF_UNIX PF_INET PF_INET6);

use parent 'OpenTelemetry::Integration';

sub dependencies { qw(Net::Async::HTTP) }

my $wanted_request_headers;
my $wanted_response_headers;

my $orig_prepare_request;
my $orig_process_response;
my $orig__do_one_request;

my sub get_headers ( $have, $want, $prefix ) {
  return unless $want;

  my %attributes;
  $have->scan( sub ( $key, $value ) {
    $key =~ tr/-/_/;
    if ($key =~ $want) {
      push @{ $attributes{ $prefix . '.' . lc $key } //= [] }, $value;
    }
  });

  %attributes;
}

fieldhash my %span;

my sub handle_request ($request) {
  my $uri    = $request->uri->clone;
  my $method = $request->method;
  my $length = length ${ $request->content_ref };

  $uri->userinfo('REDACTED:REDACTED') if $uri->userinfo;

  $span{$request} = OpenTelemetry->tracer_provider->tracer(
    name    => __PACKAGE__,
    version => __PACKAGE__->VERSION,
  )->create_span(
    name       => $method,
    kind       => SPAN_KIND_CLIENT,
    attributes => {
      # As per https://github.com/open-telemetry/semantic-conventions/blob/main/docs/http/http-spans.md
      'http.request.method'      => $method,
      'network.protocol.name'    => 'http',
      'network.protocol.version' => '1.1',
      'network.transport'        => 'tcp',
      'server.address'           => $uri->host,
      'server.port'              => $uri->port,
      'url.full'                 => "$uri", # redacted
      'url.scheme'               => $uri->scheme,
      'user_agent.original'      => $request->header('User-Agent'),

      get_headers(
        $request->headers,
        $wanted_request_headers,
        'http.request.header',
      ),
      get_headers(
        $request->headers,
        $wanted_request_headers,
        'http.request.header'
      ),
      $length ? ( 'http.request.body.size' => $length ) : (),
    },
  );

  OpenTelemetry->propagator->inject(
    $request,
    undef,
    sub { shift->header(@_) },
  );
}

my sub handle_connect ($request, $conn) {
  my $handle = $conn->read_handle;
  my $span = $span{$request}
    or return;
  my $domain = $handle->can('sockdomain') && $handle->sockdomain;
  if ($domain == PF_UNIX) {
    $span->set_attribute('network.transport'     => 'unix');
    $span->set_attribute('network.peer.address'  => $handle->peerpath);
  }
  elsif ($domain == PF_INET || $domain == PF_INET6) {
    my $proto
      = $handle->protocol == IPPROTO_TCP ? 'tcp'
      : $handle->protocol == IPPROTO_UDP ? 'udp'
      : undef;
    $span->set_attribute('network.transport'     => $proto)
      if $proto;
    $span->set_attribute('network.peer.address'  => $handle->peerhost);
    $span->set_attribute('network.peer.port'     => $handle->peerport);
  }
  elsif (!$domain) {
    $span->set_attribute('network.transport'     => 'pipe');
  }
}

my sub handle_response ($request, $response) {
  my $span = $span{$request};

  if ( my $count = $response->redirects ) {
    $span->set_attribute( 'http.resend_count' => $count )
  }

  # Net::Async::HTTP doesn't generate 599 errors, but we'll cover our bases
  if ($response->code == 599) {
    my $error = $response->content =~ s/\A\s+//r =~ s/\s+\z//r;
    my $description = $error =~ s/(?: at \S+ line \d+\.)?(?:\n|\z)//ar;
    $span->set_status( SPAN_STATUS_ERROR, $description );
    return;
  }

  if ( $response->is_success ) {
    $span->set_status( SPAN_STATUS_OK );
  }
  else {
    $span->set_status( SPAN_STATUS_ERROR, $response->code );
  }

  $span->set_attribute( 'http.response.status_code' => $response->code );

  my $length = $response->header('content-length');
  $span->set_attribute( 'http.response.body.size' => $length )
    if defined $length;

  $span->set_attribute(
    get_headers(
      $response->headers,
      $wanted_response_headers,
      'http.response.header'
    )
  );

  $span->end;
}

my sub handle_error ($request, $error) {
  my $span = $span{$request};
  my $description = $error =~ s/(?: at \S+ line \d+\.)?(?:\n|\z)//ar;
  $span->set_status( SPAN_STATUS_ERROR, $description );
  $span->end;
}

my sub _do_one_request ($self, %args) {
  my $request = $args{request};
  my $on_ready = $args{on_ready};
  $args{on_ready} = sub ($conn) {
    handle_connect($request, $conn);

    $on_ready ? $on_ready->( $conn )->then_done( $conn )
              : Future->done( $conn );
  };
  my $on_redirect = $args{on_redirect};
  $args{on_redirect} = sub ($response, $location) {
    handle_response($response->request, $response);
    $on_redirect->( $response, $location )
      if $on_ready;
    return;
  };
  $self->$orig__do_one_request(%args)->else(sub ($err) {
    handle_error($request, $err);
    Future->fail($err);
  });
}

my sub prepare_request ($self, $request, @rest) {
  my $out = $self->$orig_prepare_request($request, @rest);
  handle_request($request);
  return $out;
}

my sub process_response ($self, $response, @rest) {
  my $out = $self->$orig_process_response($response, @rest);
  handle_response($response->request, $response);
  return $out;
}

sub install ( $class, %config ) {
  $wanted_request_headers = delete $config{request_headers} // [];
  $wanted_response_headers = delete $config{response_headers} // [];

  for my $wanted ($wanted_request_headers, $wanted_response_headers) {
    if ($wanted) {
      ($wanted) =
        map qr/\A(?:$_)\z/,
        join '|',
        map quotemeta,
        map tr/-/_/r,
        @$wanted;
    }
    undef $wanted;
  }

  return unless $INC{'Net/Async/HTTP.pm'};
  no strict 'refs';
  no warnings 'redefine';
  if (!$orig_prepare_request) {
    $orig_prepare_request = \&Net::Async::HTTP::prepare_request;
    *Net::Async::HTTP::prepare_request = \&prepare_request;
  }
  if (!$orig_process_response) {
    $orig_process_response = \&Net::Async::HTTP::process_response;
    *Net::Async::HTTP::process_response = \&process_response;
  }
  if (!$orig__do_one_request) {
    $orig__do_one_request = \&Net::Async::HTTP::_do_one_request;
    *Net::Async::HTTP::_do_one_request = \&_do_one_request;
  }
  return 1;
}

sub uninstall ( $class ) {
  no strict 'refs';
  no warnings 'redefine';
  if ($orig_prepare_request) {
    *{'Net::Async::HTTP::prepare_request'} = $orig_prepare_request;
    undef $orig_prepare_request;
  }
  if ($orig_process_response) {
    *{'Net::Async::HTTP::process_response'} = $orig_process_response;
    undef $orig_process_response;
  }
  if ($orig__do_one_request) {
    *{'Net::Async::HTTP::_do_one_request'} = $orig__do_one_request;
    undef $orig__do_one_request;
  }
  return;
}

1;
__END__

=head1 NAME

OpenTelemetry::Integration::Net::Async::HTTP - A new module

=head1 SYNOPSIS

  use OpenTelemetry::Integration qw(Net::Async::HTTP);

=head1 DESCRIPTION

Adds OpenTelementry to Net::Async::HTTP.

=head1 AUTHOR

haarg - Graham Knop (cpan:HAARG) <haarg@haarg.org>

=head1 CONTRIBUTORS

None so far.

=head1 COPYRIGHT

Copyright (c) 2024 the OpenTelemetry::Integration::Net::Async::HTTP L</AUTHOR> and L</CONTRIBUTORS>
as listed above.

=head1 LICENSE

This library is free software and may be distributed under the same terms
as perl itself. See L<https://dev.perl.org/licenses/>.

=cut
