use strict;
use warnings;
use utf8;
use ZMQ::Raw;
use JSON;
use Getopt::Long;
use DBI;
use Compress::LZ4;

$| = 1;

my $connectstr = 'tcp://127.0.0.1:5556';
my $dsn = 'DBI:MariaDB:database=eosio;host=localhost';
my $db_user = 'eosio';
my $db_password = 'guugh3Ei';
my $commit_every = 1000;

my %blacklist = ('blocktwitter' => 1);
my @blacklist_acc;

my $skip_compress;

my $ok = GetOptions
    ('connect=s' => \$connectstr,
     'dsn=s'     => \$dsn,
     'dbuser=s'  => \$db_user,
     'dbpw=s'    => \$db_password,
     'bl=s'      => \@blacklist_acc,
     'plain'     => \$skip_compress);


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
    "  --bl=ACCOUNT       blacklist oone or more accounts\n",
    "  --plain            store uncompressd JSON traces\n";
    exit 1;
}

foreach my $bl ( @blacklist_acc )
{
    $blacklist{$bl} = 1;
}


my $dbh = DBI->connect($dsn, $db_user, $db_password,
                       {'RaiseError' => 1, AutoCommit => 0,
                        mariadb_server_prepare => 1});
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
     '(global_seq, account_name, contract, currency, amount, deleted) ' .
     'VALUES(?,?,?,?,?,?)');

my $sth_inslastcurr = $dbh->prepare
    ('INSERT INTO EOSIO_LATEST_CURRENCY ' .
     '(account_name, global_seq, contract, currency, amount, deleted) ' .
     'VALUES(?,?,?,?,?,?) ' .
     'ON DUPLICATE KEY UPDATE global_seq=?, amount=?, deleted=?');

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


my $ctxt = ZMQ::Raw::Context->new;
my $socket = ZMQ::Raw::Socket->new ($ctxt, ZMQ::Raw->ZMQ_PULL );
$socket->setsockopt(ZMQ::Raw::Socket->ZMQ_RCVBUF, 10240);
$socket->connect( $connectstr );

my $sighandler = sub {
    print STDERR ("Disconnecting the ZMQ socket\n");
    $socket->disconnect($connectstr);
    $socket->close();
    $dbh->commit();
    $dbh->disconnect();
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


while(1)
{
    $uncommitted++;
    my $data = $socket->recv();
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

        $sth_insaction->bind_param(1, $seq);
        $sth_insaction->bind_param(2, $block_num);
        $sth_insaction->bind_param(3, $block_time);
        $sth_insaction->bind_param(4, $actor);
        $sth_insaction->bind_param(5, $action->{'action_trace'}{'receipt'}{'receiver'});
        $sth_insaction->bind_param(6, $action_name);
        $sth_insaction->bind_param(7, $tx);
        if( $skip_compress )
        {
            $sth_insaction->bind_param(8, $js, DBI::SQL_BLOB);
        }
        else
        {
            $sth_insaction->bind_param(8, pack('Aa*', 'z', compress($js)), DBI::SQL_BLOB);
        }
        $sth_insaction->execute();
    
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
            $uncommitted+=3;
        }

        my %bal;

        foreach my $brow (@{$action->{'currency_balances'}})
        {
            my $account = $brow->{'account_name'};
            my $contract = $brow->{'contract'};
            my ($amount, $currency) = split(/\s+/, $brow->{'balance'});
            my $deleted = $brow->{'deleted'}?1:0;
            
            $bal{$account}{$contract}{$currency} = $amount;

            $sth_inscurr->execute($seq,
                                  $account,
                                  $contract,
                                  $currency,
                                  $amount,
                                  $deleted);

            $sth_inslastcurr->execute($account,
                                      $seq,
                                      $contract,
                                      $currency,
                                      $amount,
                                      $deleted,
                                      $seq,
                                      $amount,
                                      $deleted);
            $uncommitted+=2;
        }

        my $atrace = $action->{'action_trace'};
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
            
            $uncommitted++;
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
                $uncommitted += $commit_every;
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
        $uncommitted += 10;
    }
    elsif( $msgtype == 2 )  # fork
    {
        my $data = $json->decode($js);
        my $block_num = $data->{'invalid_block_num'};
        printf STDERR ("Fork event received at block number %d\n", $block_num);
        $sth_wipe_block->execute($block_num);
        $sth_wipe_block_actions->execute($block_num);
        $uncommitted += $commit_every;
    }

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

            if( ref($data) eq 'HASH' )
            {
                if( ($aname eq 'transfer' or $aname eq 'issue') and 
                    defined($data->{'quantity'}) and defined($data->{'to'}) )
                {
                    my ($amount, $currency) = split(/\s+/, $data->{'quantity'});
                    if( defined($amount) and defined($currency) and
                        $amount =~ /^[0-9.]+$/ and $currency =~ /^[A-Z]{1,7}$/ )
                    {
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
                elsif( $aname eq 'open' and defined($data->{'owner'}) and
                       defined($data->{'symbol'}) and $data->{'symbol'} =~ /^[A-Z]{1,7}$/ )
                {
                        push(@{$state->{'transfers'}}, {
                            'global_seq' => $gseq,
                            'contract' => $act->{'account'},
                            'currency' => $data->{'symbol'},
                            'amount' => 0,
                            'tx_from' => undef,
                            'tx_to' => $data->{'owner'},
                            'memo' => '',
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
