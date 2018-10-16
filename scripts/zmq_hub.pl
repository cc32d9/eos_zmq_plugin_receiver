use strict;
use warnings;
use ZMQ::LibZMQ3;
use ZMQ::Constants ':all';
use Getopt::Long;

$| = 1;

my $ep_pull;
my @ep_push;
my @ep_pub;
my $rcvbuf = 5000;

my $ok = GetOptions
    ('pull=s'    => \$ep_pull,
     'push=s'    => \@ep_push,
     'pub=s'     => \@ep_pub,
     'buf=i'     => \$rcvbuf);

if( not $ok or scalar(@ARGV) > 0 or not $ep_pull or
    scalar(@ep_push)+scalar(@ep_pub) == 0 )
{
    print STDERR "Usage: $0 --pull=ENDPOINT [options...]\n",
    "The utility connects to ZMQ PUSH socket and \n",
    "copies the messages to PUSH or PUB sockets.\n",
    "At least one push or pub socket needs to be defined.\n",
    "Multiple push and pub sockets can be defined.\n",
    "Options:\n",
    "  --pull=ENDPOINT\n",
    "  --push=ENDPOINT\n",
    "  --pub=ENDPOINT\n";
    exit 1;
}


my $ctxt = zmq_init;


my $s_pull = zmq_socket( $ctxt, ZMQ_PULL );
my $rv = zmq_setsockopt( $s_pull, ZMQ_RCVHWM, $rcvbuf );
die($!) if $rv;
$rv = zmq_connect( $s_pull, $ep_pull );
die($!) if $rv;

my @s_push;
my @s_pub;
my %connections;

foreach my $ep (@ep_push)
{
    my $s =  zmq_socket( $ctxt, ZMQ_PUSH );
    $rv = zmq_bind( $s, $ep );
    die($!) if $rv;
    push(@s_push, $s);
    $connections{$ep} = $s;
}

foreach my $ep (@ep_pub)
{
    my $s = zmq_socket( $ctxt, ZMQ_PUB );
    my $rv = zmq_setsockopt( $s, ZMQ_LINGER, 0 );
    die($!) if $rv;
    $rv = zmq_setsockopt( $s, ZMQ_SNDTIMEO, 0 );
    die($!) if $rv;
    $rv = zmq_bind( $s, $ep );
    die($!) if $rv;
    push(@s_pub, $s);
    $connections{$ep} = $s;
}
    

my $sighandler = sub {
    print STDERR ("Disconnecting all ZMQ sockets\n");
    foreach my $ep (keys %connections)
    {
        zmq_disconnect($connections{$ep}, $ep);
        zmq_close($connections{$ep});
    }
    print STDERR ("Finished\n");
    exit;
};


$SIG{'HUP'} = $sighandler;
$SIG{'TERM'} = $sighandler;
$SIG{'INT'} = $sighandler;


my $inmsg = zmq_msg_init();
while( zmq_msg_recv($inmsg, $s_pull) != -1 )
{
    my $indata = zmq_msg_data( $inmsg );
    foreach my $s (@s_push, @s_pub)
    {
        my $outmsg = zmq_msg_init_data($indata);
        zmq_msg_send($outmsg, $s);
    }
}


print STDERR "The stream ended\n";

