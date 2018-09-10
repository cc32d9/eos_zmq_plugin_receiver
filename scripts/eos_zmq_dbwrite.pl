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
my $commit_every = 10;

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

my $sth_check_tx = $dbh->prepare
    ('SELECT global_action_seq, trx_id FROM EOSIO_ACTIONS ' .
     'WHERE global_action_seq=? OR trx_id=?');

my $sth_del_act = $dbh->prepare
    ('DELETE FROM EOSIO_ACTIONS WHERE global_action_seq=?');

my $sth_insaction = $dbh->prepare
    ('INSERT INTO EOSIO_ACTIONS ' . 
     '(global_action_seq, block_num, block_time,' .
     'actor_account, recipient_account, action_name, trx_id, jsdata) ' .
     'VALUES(?,?,?,?,?,?,?,?)');

my $sth_del_inline_seq = $dbh->prepare
    ('DELETE FROM EOSIO_INLINE_ACTION_SEQ WHERE master_global_seq=?'); 

my $sth_ins_inline_seq = $dbh->prepare
    ('INSERT INTO EOSIO_INLINE_ACTION_SEQ ' . 
     '(global_seq, master_global_seq) VALUES(?,?)');

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

my $sth_upd_irreversible = $dbh->prepare
    ('UPDATE EOSIO_VARS SET val_int=? where varname=\'last_irreversible_block\'');


my $ctxt = zmq_init;
my $socket = zmq_socket( $ctxt, ZMQ_PULL );

my $rv = zmq_connect( $socket, $connectstr );
die($!) if $rv;


my $json = JSON->new->pretty->canonical;
my $n_commit_block = 0;

my $msg = zmq_msg_init();
while( zmq_msg_recv($msg, $socket) != -1 )
{
    my $data = zmq_msg_data($msg);
    my ($msgtype, $opts, $js) = unpack('VVa*', $data);
    my $action = $json->decode($js);

    if( $action->{'block_num'} >= $n_commit_block + $commit_every )
    {
        $n_commit_block = $action->{'block_num'};
        $dbh->commit();
    }

    my $actor = $action->{'action_trace'}{'act'}{'account'};
    next if $blacklist{$actor};

    my $tx = $action->{'action_trace'}{'trx_id'};
    my $seq = $action->{'global_action_seq'};

    my $seqs = {};
    collect_seqs($action->{'action_trace'}, $seqs);
        
    my $skip;
    
    $sth_check_tx->execute($seq, $tx);
    my $r = $sth_check_tx->fetchall_arrayref();
    if( scalar(@{$r}) > 0 )
    {
        foreach my $row (@{$r})
        {
            my $dbseq = $row->[0];
            my $dbtx = $row->[1];
            if( $dbseq == $seq and $dbtx eq $tx )
            {
                $skip = 1;
            }
            else
            {
                $sth_del_act->execute($dbseq);
            }
        }
    }

    # a transaction from reversible block may never end up in
    # irreversible blocks, so we erase all transactions that conflict
    # with our sequence numbers
    
    $r = $dbh->selectall_arrayref
        ('SELECT master_global_seq FROM EOSIO_INLINE_ACTION_SEQ ' .
         'WHERE global_seq IN(' . join(',', keys %{$seqs}) . ')');

    foreach my $row (@{$r})
    {
        my $dbseq = $row->[0];
        if( $dbseq != $seq )
        {
            $sth_del_act->execute($dbseq);
        }
    }

    $sth_del_inline_seq->execute($seq);
    
    if( not $skip )
    {
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
    }

    foreach my $iseq (keys %{$seqs})
    {
        $sth_ins_inline_seq->execute($iseq, $seq);
    }    
    
    $sth_upd_irreversible->execute($action->{'last_irreversible_block'});
}


print STDERR "The stream ended\n";
$dbh->disconnect();


sub collect_seqs
{
    my $atrace = shift;
    my $seqs = shift;

    $seqs->{$atrace->{'receipt'}{'global_sequence'}} = 1;

    if( defined($atrace->{'inline_traces'}) )
    {
        foreach my $trace (@{$atrace->{'inline_traces'}})
        {
            collect_seqs($trace, $seqs);
        }
    }
}

