use strict;
use warnings;
use ZMQ::LibZMQ3;
use ZMQ::Constants ':all';
use JSON;
use Getopt::Long;


$| = 1;

my $ep_pull;
my $ep_sub;

my $ok = GetOptions
    ('pull=s' => \$ep_pull,
     'sub=s'  => \$ep_sub);


if( not $ok or scalar(@ARGV) > 0 or
    (not $ep_pull and not $ep_sub) or
    ($ep_pull and $ep_sub) )
{
    print STDERR "Usage: $0 [options...]\n",
    "The utility connects to EOS ZMQ PUSH or PUB socket and \n",
    "dumps incoming messages to stdout\n",
    "Options:\n",
    "  --pull=ENDPOINT  connect to a PUSH socket\n",
    "  --sub=ENDPOINT   connect to a PUB socket\n";
    exit 1;
}



my $ctxt = zmq_init;
my $socket;

if( defined($ep_pull) )
{
    $socket = zmq_socket($ctxt, ZMQ_PULL);
    my $rv = zmq_connect( $socket, $ep_pull );
    die($!) if $rv;
}
else
{
    $socket = zmq_socket($ctxt, ZMQ_SUB);
    my $rv = zmq_connect( $socket, $ep_sub );
    die($!) if $rv;
    $rv = zmq_setsockopt( $socket, ZMQ_SUBSCRIBE, '' );
    die($!) if $rv;
}    


my $json = JSON->new->pretty->canonical;

my $msg = zmq_msg_init();
while( zmq_msg_recv($msg, $socket) != -1 )
{
    my $data = zmq_msg_data($msg);
    my ($msgtype, $opts, $js) = unpack('VVa*', $data);
    my $action = $json->decode($js);
    printf("%d %d\n", $msgtype, $opts);
    print $json->encode($action);
    print "\n";
}

print "The stream ended\n";
