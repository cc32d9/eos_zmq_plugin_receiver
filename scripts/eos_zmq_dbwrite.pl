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

my $sth_check_block = $dbh->prepare
    ('SELECT block_digest from EOSIO_BLOCKS WHERE block_num=?');

my $sth_ins_block = $dbh->prepare
    ('INSERT INTO EOSIO_BLOCKS (block_num, block_digest) VALUES(?,?)');

my $sth_wipe_block = $dbh->prepare
    ('DELETE FROM EOSIO_BLOCKS WHERE block_num >= ?');

my $sth_wipe_block_actions = $dbh->prepare
    ('DELETE FROM EOSIO_ACTIONS WHERE block_num >= ?');


my $sth_insaction = $dbh->prepare
    ('INSERT IGNORE INTO EOSIO_ACTIONS ' .
     '(global_action_seq, block_num, block_time,' .
     'actor_account, recipient_account, action_name, trx_id, jsdata) ' .
     'VALUES(?,?,?,?,?,?,?,?)');

my $sth_upd_irrev = $dbh->prepare
    ('UPDATE EOSIO_ACTIONS SET irreversible=1 WHERE block_num=?');

my $sth_insres = $dbh->prepare
    ('INSERT IGNORE INTO EOSIO_RESOURCE_BALANCES ' .
     '(global_seq, account_name, ' .
     'cpu_weight, cpu_used, cpu_available, cpu_max, ' .
     'net_weight, net_used, net_available, net_max, ram_quota, ram_usage) ' .
     'VALUES(?,?,?,?,?,?,?,?,?,?,?,?)');

my $sth_inslastres = $dbh->prepare
    ('INSERT INTO EOSIO_LATEST_RESOURCE ' .
     '(account_name, global_seq, ' .
     'cpu_weight, cpu_used, cpu_available, cpu_max, ' .
     'net_weight, net_used, net_available, net_max, ram_quota, ram_usage) ' .
     'VALUES(?,?,?,?,?,?,?,?,?,?,?,?) ' .
     'ON DUPLICATE KEY UPDATE global_seq=?, ' .
     'cpu_weight=?, cpu_used=?, cpu_available=?, cpu_max=?, ' .
     'net_weight=?, net_used=?, net_available=?, net_max=?, ' .
     'ram_quota=?, ram_usage=?');

my $sth_inscurr = $dbh->prepare
    ('INSERT IGNORE INTO EOSIO_CURRENCY_BALANCES ' .
     '(global_seq, account_name, contract, currency, amount) ' .
     'VALUES(?,?,?,?,?)');

my $sth_inslastcurr = $dbh->prepare
    ('INSERT INTO EOSIO_LATEST_CURRENCY ' .
     '(account_name, global_seq, contract, currency, amount) ' .
     'VALUES(?,?,?,?,?) ' .
     'ON DUPLICATE KEY UPDATE global_seq=?, amount=?');

my $sth_upd_last_irreversible = $dbh->prepare
    ('UPDATE EOSIO_VARS SET val_int=? where varname=\'last_irreversible_block\'');


my $sth_add_transfer = $dbh->prepare
    ('INSERT IGNORE INTO EOSIO_TRANSFERS ' .
     '(seq, global_seq, block_num, block_time, trx_id, ' .
     'contract, currency, amount, tx_from, tx_to, memo, bal_from, bal_to) ' .
     'VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)');

my $sth_add_history = $dbh->prepare
    ('INSERT IGNORE INTO EOSIO_ACC_HISTORY ' .
     '(global_seq, block_num, block_time, trx_id, ' .
     'account_name, contract, action_name) ' .
     'VALUES(?,?,?,?,?,?,?)');


my $ctxt = zmq_init;
my $socket = zmq_socket( $ctxt, ZMQ_PULL );

my $rv = zmq_connect( $socket, $connectstr );
die($!) if $rv;

my $sighandler = sub {
    print STDERR ("Disconnecting the ZMQ socket\n");
    zmq_disconnect($socket, $connectstr);
    zmq_close($socket);
    print STDERR ("Finished\n");
    exit;
};


$SIG{'HUP'} = $sighandler;
$SIG{'TERM'} = $sighandler;
$SIG{'INT'} = $sighandler;


my $json = JSON->new->pretty->canonical;
my $uncommitted = 0;

# if the block with the same number and digest is in the database, skip inserting actions
my $block_already_in_db = 0;

my $msg = zmq_msg_init();
while( zmq_msg_recv($msg, $socket) != -1 )
{
    my $data = zmq_msg_data($msg);
    my ($msgtype, $opts, $js) = unpack('VVa*', $data);
    if( $msgtype == 0 )  # action and balances
    {
        next if $block_already_in_db;

        my $action = $json->decode($js);

        my $actor = $action->{'action_trace'}{'act'}{'account'};
        next if $blacklist{$actor};

        my $tx = $action->{'action_trace'}{'trx_id'};
        my $seq = $action->{'global_action_seq'};

        my $block_num = $action->{'block_num'};

        my $block_time =  $action->{'block_time'};
        $block_time =~ s/T/ /;

        my $action_name = $action->{'action_trace'}{'act'}{'name'};

        $sth_insaction->execute($seq,
                                $block_num,
                                $block_time,
                                $actor,
                                $action->{'action_trace'}{'receipt'}{'receiver'},
                                $action_name,
                                $tx,
                                $js);

        foreach my $brow (@{$action->{'resource_balances'}})
        {
            my $account = $brow->{'account_name'};

            my $cpuw = $brow->{'cpu_weight'}/10000.0;
            my $cpulu = $brow->{'cpu_limit'}{'used'};
            my $cpula = $brow->{'cpu_limit'}{'available'};
            my $cpulm = $brow->{'cpu_limit'}{'max'};

            my $netw = $brow->{'net_weight'}/10000.0;
            my $netlu = $brow->{'net_limit'}{'used'};
            my $netla = $brow->{'net_limit'}{'available'};
            my $netlm = $brow->{'net_limit'}{'max'};

            my $quota = $brow->{'ram_quota'};
            my $usage = $brow->{'ram_usage'};

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

            $sth_add_history->execute($seq,
                                      $block_num,
                                      $block_time,
                                      $tx,
                                      $account,
                                      $actor,
                                      $action_name);
        }

        my %bal;

        foreach my $brow (@{$action->{'currency_balances'}})
        {
            my $account = $brow->{'account_name'};
            my $contract = $brow->{'contract'};
            my ($amount, $currency) = split(/\s+/, $brow->{'balance'});

            $bal{$account}{$contract}{$currency} = $amount;

            $sth_inscurr->execute($seq,
                                  $account,
                                  $contract,
                                  $currency,
                                  $amount);

            $sth_inslastcurr->execute($account,
                                      $seq,
                                      $contract,
                                      $currency,
                                      $amount,
                                      $seq,
                                      $amount);
        }

        my $atrace = $action->{'action_trace'};
        my $data = $atrace->{'act'}{'data'};
        my $state = {'transfers' => []};
        retrieve_transfers($atrace, $state);

        foreach my $transfer (@{$state->{'transfers'}})
        {
            my $gseq = $transfer->{'global_seq'};
            my $contract = $transfer->{'contract'};
            my $currency = $transfer->{'currency'};

            my $to = $transfer->{'tx_to'};
            next unless defined($to);

            my $bal_to = $bal{$to}{$contract}{$currency};
            next unless defined($bal_to);

            my $from = $transfer->{'tx_from'};
            my $bal_from = defined($from) ? $bal{$from}{$contract}{$currency} : undef;
            next if( defined($from) and not defined($bal_from) );

            $sth_add_transfer->execute
                ($gseq,
                 $seq,
                 $block_num,
                 $block_time,
                 $tx,
                 $contract,
                 $currency,
                 $transfer->{'amount'},
                 $from,
                 $to,
                 $transfer->{'memo'},
                 $bal_from,
                 $bal_to,
                );
        }
    }
    elsif( $msgtype == 3 )  # accepted block
    {
        my $data = $json->decode($js);
        my $block_num = $data->{'accepted_block_num'};
        my $digest = $data->{'accepted_block_digest'};

        $block_already_in_db = 0;

        $sth_check_block->execute($block_num);
        my $r = $sth_check_block->fetchall_arrayref();
        if( scalar(@{$r}) > 0 )
        {
            if( $r->[0][0] ne $digest )
            {
                printf STDERR ("Fork detected at block number %d\n", $block_num);
                $sth_wipe_block->execute($block_num);
                $sth_wipe_block_actions->execute($block_num);
            }
            else
            {
                $block_already_in_db = 1;
            }
        }

        if( not $block_already_in_db )
        {
            $sth_ins_block->execute($block_num, $digest);
        }
    }
    elsif( $msgtype == 1 )  # irreversible block
    {
        my $data = $json->decode($js);
        my $block_num = $data->{'irreversible_block_num'};

        $sth_upd_last_irreversible->execute($block_num);
        $sth_upd_irrev->execute($block_num);
    }
    elsif( $msgtype == 2 )  # fork
    {
        my $data = $json->decode($js);
        my $block_num = $data->{'invalid_block_num'};
        printf STDERR ("Fork event received at block number %d\n", $block_num);
        $sth_wipe_block->execute($block_num);
        $sth_wipe_block_actions->execute($block_num);
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
exit(0);


sub retrieve_transfers
{
    my $atrace = shift;
    my $state = shift;

    my $gseq = $atrace->{'receipt'}{'global_sequence'};
    if( not defined($state->{'seqs'}{$gseq}) )
    {
        $state->{'seqs'}{$gseq} = 1;
        my $act = $atrace->{'act'};

        if( $atrace->{'receipt'}{'receiver'} eq $act->{'account'} )
        {
            my $aname = $act->{'name'};
            my $data = $act->{'data'};

            if( ref($data) eq 'HASH' and
                ($aname eq 'transfer' or $aname eq 'issue' or $aname eq 'create') )
            {
                if( defined($data->{'quantity'}) and defined($data->{'to'}) )
                {
                    my ($amount, $currency) = split(/\s+/, $data->{'quantity'});

                    push(@{$state->{'transfers'}}, {
                        'global_seq' => $gseq,
                        'contract' => $act->{'account'},
                        'currency' => $currency,
                        'amount' => $amount,
                        'tx_from' => $data->{'from'},
                        'tx_to' => $data->{'to'},
                        'memo' => $data->{'memo'},
                         });
                }
            }
        }
    }

    if( defined($atrace->{'inline_traces'}) )
    {
        foreach my $trace (@{$atrace->{'inline_traces'}})
        {
            retrieve_transfers($trace, $state);
        }
    }
}
