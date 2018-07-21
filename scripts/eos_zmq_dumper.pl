use strict;
use warnings;
use ZMQ::LibZMQ3;
use ZMQ::Constants ':all';
use JSON;
use Getopt::Long;


$| = 1;

my $connectstr = 'tcp://127.0.0.1:5556';

my $ok = GetOptions ('connect=s' => \$connectstr);


if( not $ok or scalar(@ARGV) > 0 )
{
    print STDERR "Usage: $0 [options...]\n",
    "The utility connects to EOS ZMQ PUSH socket and \n",
    "dumps incoming messages to stdout\n",
    "Options:\n",
    "  --connect=ENDPOINT \[$connectstr\]\n";
    exit 1;
}



my $ctxt = zmq_init;
my $socket = zmq_socket( $ctxt, ZMQ_PULL );

my $rv = zmq_connect( $socket, $connectstr );
die($!) if $rv;


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
