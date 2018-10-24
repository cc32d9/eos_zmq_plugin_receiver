use strict;
use warnings;
use ZMQ::Raw;
use ZMQ::Constants ':all';
use JSON;
use Getopt::Long;


$| = 1;

my $ep_pull;
my $ep_sub;
my $short;

my $ok = GetOptions
    ('pull=s' => \$ep_pull,
     'sub=s'  => \$ep_sub,
     'short'  => \$short);


if( not $ok or scalar(@ARGV) > 0 or
    (not $ep_pull and not $ep_sub) or
    ($ep_pull and $ep_sub) )
{
    print STDERR "Usage: $0 [options...]\n",
    "The utility connects to EOS ZMQ PUSH or PUB socket and \n",
    "dumps incoming messages to stdout\n",
    "Options:\n",
    "  --pull=ENDPOINT  connect to a PUSH socket\n",
    "  --sub=ENDPOINT   connect to a PUB socket\n",
    "  --short          display only brief information\n";
    exit 1;
}



my $ctxt = ZMQ::Raw::Context->new;
my $socket;
my $connectstr;

if( defined($ep_pull) )
{
    $connectstr = $ep_pull;
    $socket = ZMQ::Raw::Socket->new ($ctxt, ZMQ::Raw->ZMQ_PULL );
    $socket->setsockopt(ZMQ::Raw::Socket->ZMQ_RCVBUF, 10240);
    $socket->connect( $connectstr );
}
else
{
    $connectstr = $ep_sub;
    $socket = ZMQ::Raw::Socket->new ($ctxt, ZMQ::Raw->ZMQ_SUB );
    $socket->setsockopt(ZMQ::Raw::Socket->ZMQ_RCVBUF, 10240);
    $socket->setsockopt(ZMQ::Raw::Socket->ZMQ_SUBSCRIBE, '' );
    $socket->connect( $connectstr );
}    


my $sighandler = sub {
    print STDERR ("Disconnecting the ZMQ socket\n");
    $socket->disconnect($connectstr);
    $socket->close();
    print STDERR ("Finished\n");
    exit;
};


$SIG{'HUP'} = $sighandler;
$SIG{'TERM'} = $sighandler;
$SIG{'INT'} = $sighandler;


my $json = JSON->new->pretty->canonical;

while(1)
{
    my $data = $socket->recv();
    my ($msgtype, $opts, $js) = unpack('VVa*', $data);
    $data = $json->decode($js);
    if( $short )
    {
        my $out = $msgtype . ' ' .  $opts . ' ';
        if( $msgtype == 0 )
        {
            $out .= $data->{'block_time'} . ' ' .
                substr($data->{'action_trace'}{'trx_id'}, 0, 8) . ' ' .
                $data->{'action_trace'}{'act'}{'account'} . ' ' .
                $data->{'action_trace'}{'act'}{'name'};
        }
        elsif( $msgtype == 3 )  # accepted block
        {
            $out .= 'accepted ' . $data->{'accepted_block_num'};
        }
        elsif( $msgtype == 1 )  # irreversible block
        {
            $out .= 'irreversible ' . $data->{'irreversible_block_num'};
        }
        elsif( $msgtype == 2 )  # fork
        {
            $out .= 'fork ' . $data->{'invalid_block_num'};
        }
        elsif( $msgtype == 4 )  # failed
        {
            $out .= 'failed ' . $data->{'block_num'} . ' ' .
                substr($data->{'trx_id'}, 0, 8) . ' ' . $data->{'status_name'};
        }

        print $out, "\n";
    }
    else
    {
        printf("%d %d\n", $msgtype, $opts);
        print $json->encode($data);
        print "\n";
    }
}

print "The stream ended\n";
