use strict;
use warnings;
use ZMQ::LibZMQ3;
use ZMQ::Constants ':all';
use JSON;
use Getopt::Long;
use DBI;


my $connectstr = 'tcp://127.0.0.1:5556';
my $dsn = 'DBI:mysql:database=eosio;host=localhost';
my $db_user = 'eosio';
my $db_password = 'guugh3Ei';
my $commit_every = 100;

my %blacklist = ('blocktwitter' => 1);
my @blacklist_acc;


my $ok = GetOptions
    ('connect=s' => \$connectstr,
     'dsn=s'     => \$dsn,
     'dbuser=s'  => \$db_user,
     'dbpw=s'    => \$db_password,
     'bl=s'      => \@blacklist_acc);


if( not $ok or scalar(@ARGV) > 0 )
{
    print STDERR "Usage: $0 [options...]\n",
    "The utility connects to EOS ZMQ PUSH socket and \n",
    "inserts the messages in the database\n",
    "Options:\n",
    "  --connect=ENDPOINT \[$connectstr\]\n",
    "  --dsn=DSN          \[$dsn\]\n",
    "  --dbuser=USER      \[$db_user\]\n",
    "  --dbpw=PASSWORD    \[$db_password\]\n",
    "  --bl=ACCOUNT       blacklist oone or more accounts\n";
    exit 1;
}

foreach my $bl ( @blacklist_acc )
{
    $blacklist{$bl} = 1;
}
        

my $dbh = DBI->connect($dsn, $db_user, $db_password,
                       {'RaiseError' => 1, AutoCommit => 0,
                        mysql_server_prepare => 1});
die($DBI::errstr) unless $dbh;

my $sth_insaction = $dbh->prepare
    ('INSERT INTO EOSIO_ACTIONS ' . 
     '(global_action_seq, block_num, block_time,' .
     'actor_account, recipient_account, action_name, trx_id, jsdata) ' .
     'VALUES(?,?,?,?,?,?,?,?)');

my $sth_insres = $dbh->prepare
    ('INSERT INTO EOSIO_RESOURCE_BALANCES ' . 
     '(global_action_seq, account_name, ' .
     'cpu_weight, cpu_used, cpu_available, cpu_max, ' .
     'net_weight, net_used, net_available, net_max, ram_quota, ram_usage) ' .
     'VALUES(?,?,?,?,?,?,?,?,?,?,?,?)');

my $sth_inslastres = $dbh->prepare
    ('INSERT INTO EOSIO_LATEST_RESOURCE ' . 
     '(account_name, global_action_seq, ' .
     'cpu_weight, cpu_used, cpu_available, cpu_max, ' .
     'net_weight, net_used, net_available, net_max, ram_quota, ram_usage) ' .
     'VALUES(?,?,?,?,?,?,?,?,?,?,?,?) ' .
     'ON DUPLICATE KEY UPDATE global_action_seq=?, ' .
     'cpu_weight=?, cpu_used=?, cpu_available=?, cpu_max=?, ' .
     'net_weight=?, net_used=?, net_available=?, net_max=?, ' .
     'ram_quota=?, ram_usage=?');

my $sth_inscurr = $dbh->prepare
    ('INSERT INTO EOSIO_CURRENCY_BALANCES ' . 
     '(global_action_seq, account_name, issuer, currency, amount) ' .
     'VALUES(?,?,?,?,?)');

my $sth_inslastcurr = $dbh->prepare
    ('INSERT INTO EOSIO_LATEST_CURRENCY ' . 
     '(account_name, global_action_seq, issuer, currency, amount) ' .
     'VALUES(?,?,?,?,?) ' .
     'ON DUPLICATE KEY UPDATE global_action_seq=?, amount=?');

my $sth_upd_last_irreversible = $dbh->prepare
    ('UPDATE EOSIO_VARS SET val_int=? where varname=\'last_irreversible_block\'');


my $ctxt = zmq_init;
my $socket = zmq_socket( $ctxt, ZMQ_PULL );

my $rv = zmq_connect( $socket, $connectstr );
die($!) if $rv;


my $json = JSON->new->pretty->canonical;
my $uncommitted = 0;

my $msg = zmq_msg_init();
while( zmq_msg_recv($msg, $socket) != -1 )
{
    my $data = zmq_msg_data($msg);
    my ($msgtype, $opts, $js) = unpack('VVa*', $data);
    if( $msgtype == 0 )  # action and balances
    {
        my $action = $json->decode($js);
    
        my $actor = $action->{'action_trace'}{'act'}{'account'};
        next if $blacklist{$actor};

        my $tx = $action->{'action_trace'}{'trx_id'};
        my $seq = $action->{'global_action_seq'};

        my $block_time =  $action->{'block_time'};
        $block_time =~ s/T/ /;

        $sth_insaction->execute($seq,
                                $action->{'block_num'},
                                $block_time,
                                $actor,
                                $action->{'action_trace'}{'receipt'}{'receiver'},
                                $action->{'action_trace'}{'act'}{'name'},
                                $action->{'action_trace'}{'trx_id'},
                                $js);
        
        foreach my $bal (@{$action->{'resource_balances'}})
        {
            my $account = $bal->{'account_name'};
            
            my $cpuw = $bal->{'cpu_weight'}/10000.0;
            my $cpulu = $bal->{'cpu_limit'}{'used'};
            my $cpula = $bal->{'cpu_limit'}{'available'};
            my $cpulm = $bal->{'cpu_limit'}{'max'};
            
            my $netw = $bal->{'net_weight'}/10000.0;
            my $netlu = $bal->{'net_limit'}{'used'};
            my $netla = $bal->{'net_limit'}{'available'};
            my $netlm = $bal->{'net_limit'}{'max'};
            
            my $quota = $bal->{'ram_quota'};
            my $usage = $bal->{'ram_usage'};
            
            $sth_insres->execute($seq,
                                 $account,
                                 $cpuw, $cpulu, $cpula, $cpulm,
                                 $netw, $netlu, $netla, $netlm,
                                 $quota,
                                 $usage);
            
            $sth_inslastres->execute($account,
                                     $seq,
                                     $cpuw, $cpulu, $cpula, $cpulm,
                                     $netw, $netlu, $netla, $netlm,
                                     $quota,
                                     $usage,
                                     $seq,
                                     $cpuw, $cpulu, $cpula, $cpulm,
                                     $netw, $netlu, $netla, $netlm,
                                     $quota,
                                     $usage);
        }
        
        foreach my $bal (@{$action->{'currency_balances'}})
        {
            my $account = $bal->{'account_name'};
            my $issuer = $bal->{'issuer'};
            my ($amount, $currency) = split(/\s+/, $bal->{'balance'});
            
            $sth_inscurr->execute($seq,
                                  $account,
                                  $issuer,
                                  $currency,
                                  $amount);
            
            $sth_inslastcurr->execute($account,
                                      $seq,
                                      $issuer,
                                      $currency,
                                      $amount,
                                      $seq,
                                      $amount);
        }
       
        $sth_upd_last_irreversible->execute($action->{'last_irreversible_block'});
    }
    elsif( $msgtype == 1 )  # irreversible block
    {
        my $data = $json->decode($js);
        if( scalar(@{$data->{'transactions'}}) > 0 )
        {
            $dbh->do('UPDATE EOSIO_ACTIONS SET irreversible=1 ' .
                     'WHERE trx_id IN (' .
                     join(',', map {'\'' . $_ . '\''} sort @{$data->{'transactions'}}) .
                     ')');
        }
    }

    $uncommitted++;
    if( $uncommitted >= $commit_every )
    {
        $dbh->commit();
        $uncommitted = 0;
    }
}


print STDERR "The stream ended\n";
$dbh->disconnect();



