use strict;
use warnings;
use ZMQ::Raw;
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


my $ctxt = ZMQ::Raw::Context->new;

my $s_pull = ZMQ::Raw::Socket->new ($ctxt, ZMQ::Raw->ZMQ_PULL );
$s_pull->setsockopt(ZMQ::Raw::Socket->ZMQ_RCVBUF, 10240);
$s_pull->setsockopt( ZMQ::Raw::Socket->ZMQ_RCVHWM, $rcvbuf );
$s_pull->connect( $ep_pull );

my @s_push;
my @s_pub;
my %connections;

foreach my $ep (@ep_push)
{
    my $s = ZMQ::Raw::Socket->new ($ctxt, ZMQ::Raw->ZMQ_PUSH );
    $s->bind( $ep );
    push(@s_push, $s);
    $connections{$ep} = $s;
}

foreach my $ep (@ep_pub)
{
    my $s = ZMQ::Raw::Socket->new ($ctxt, ZMQ::Raw->ZMQ_PUB );
    $s->setsockopt( ZMQ::Raw::Socket->ZMQ_LINGER, 0 );
    $s->setsockopt( ZMQ::Raw::Socket->ZMQ_SNDTIMEO, 0 );
    $s->bind( $ep );
    push(@s_pub, $s);
    $connections{$ep} = $s;
}
    

my $sighandler = sub {
    print STDERR ("Disconnecting all ZMQ sockets\n");
    foreach my $ep (keys %connections)
    {
        $connections{$ep}->disconnect($ep);
        $connections{$ep}->close();
    }
    print STDERR ("Finished\n");
    exit;
};


$SIG{'HUP'} = $sighandler;
$SIG{'TERM'} = $sighandler;
$SIG{'INT'} = $sighandler;


while(1)
{
    my $indata = $s_pull->recv();
    foreach my $s (@s_push, @s_pub)
    {
        $s->send($indata);
    }
}



