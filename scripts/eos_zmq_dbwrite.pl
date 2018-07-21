use strict;
use warnings;
use ZMQ::LibZMQ3;
use ZMQ::Constants ':all';
use JSON;
use Getopt::Long;
use DBI;


$| = 1;

my $connectstr = 'tcp://127.0.0.1:5556';
my $dsn = 'DBI:mysql:database=eosio;host=localhost';
my $db_user = 'eosio';
my $db_password = 'guugh3Ei';


my $ok = GetOptions
    ('connect=s' => \$connectstr,
     'dsn=s'     => \$dsn,
     'dbuser=s'  => \$db_user,
     'dbpw=s'    => \$db_password);


if( not $ok or scalar(@ARGV) > 0 )
{
    print STDERR "Usage: $0 [options...]\n",
    "The utility connects to EOS ZMQ PUSH socket and \n",
    "inserts the messages in the database\n",
    "Options:\n",
    "  --connect=ENDPOINT \[$connectstr\]\n",
    "  --dsn=DSN          \[$dsn\]\n",
    "  --dbuser=USER      \[$db_user\]\n",
    "  --dbpw=PASSWORD    \[$db_password\]\n" ;
    exit 1;
}


my $dbh = DBI->connect($dsn, $db_user, $db_password,
                       {'RaiseError' => 1, AutoCommit => 0,
                        mysql_server_prepare => 1});
die($DBI::errstr) unless $dbh;

my $sth_insaction =
    $dbh->prepare('INSERT INTO EOSIO_ACTIONS ' . 
                  '(global_action_seq, block_num, block_time,' .
                  'actor_account, recipient_account, action_name,' .
                  'trx_id, jsdata) ' .
                  'VALUES(?,?,?,?,?,?,?,?)');

my $sth_insres =
    $dbh->prepare('INSERT INTO EOSIO_RESOURCE_BALANCES ' . 
                  '(global_action_seq, account_name,' .
                  'cpu_weight, net_weight, ram_quota, ram_usage)' .
                  'VALUES(?,?,?,?,?,?)');

my $sth_inscurr =
    $dbh->prepare('INSERT INTO EOSIO_CURRENCY_BALANCES ' . 
                  '(global_action_seq, account_name,' .
                  'issuer, currency, amount)' .
                  'VALUES(?,?,?,?,?)');

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

    my $block_time =  $action->{'block_time'};
    $block_time =~ s/T/ /;

    my $seq = $action->{'global_action_seq'};
    
    $sth_insaction->execute($seq,
                            $action->{'block_num'},
                            $block_time,
                            $action->{'action_trace'}{'act'}{'account'},
                            $action->{'action_trace'}{'receipt'}{'receiver'},
                            $action->{'action_trace'}{'act'}{'name'},
                            $action->{'action_trace'}{'trx_id'},
                            $js);

    foreach my $bal (@{$action->{'resource_balances'}})
    {
        $sth_insres->execute($seq,
                             $bal->{'account_name'},
                             $bal->{'cpu_weight'}/10000.0,
                             $bal->{'net_weight'}/10000.0,
                             $bal->{'ram_quota'},
                             $bal->{'ram_usage'});
    }

    foreach my $bal (@{$action->{'currency_balances'}})
    {
        my ($amount, $currency) = split(/\s+/, $bal->{'balance'});
        $sth_inscurr->execute($seq,
                              $bal->{'account_name'},
                              $bal->{'issuer'},
                              $currency,
                              $amount);
    }

    $dbh->commit();
}


print STDERR "The stream ended\n";
$dbh->disconnect();

