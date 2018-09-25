use strict;
use warnings;
use JSON;
use Getopt::Long;
use DBI;
use Excel::Writer::XLSX; 
use Excel::Writer::XLSX::Utility;

$| = 1;

my $rescan;

my $dsn = 'DBI:mysql:database=eosio;host=localhost';
my $db_user = 'eosio';
my $db_password = 'guugh3Ei';

my $json = JSON->new;

my $ok = GetOptions
    ('rescan'    => \$rescan,
     'dsn=s'     => \$dsn,
     'dbuser=s'  => \$db_user,
     'dbpw=s'    => \$db_password);


if( not $ok or scalar(@ARGV) > 0 )
{
    print STDERR "Usage: $0 --acc=NAME --out=FILE.xlsx --start=YYYY-MM-DD [options...]\n",
    "The utility updates EOSIO_TRANSFERS table from raw transactions.\n",
    "Options:\n",
    "  --rescan             process the whole history from the beginning\n",
    "  --dsn=DSN            \[$dsn\]\n",
    "  --dbuser=USER        \[$db_user\]\n",
    "  --dbpw=PASSWORD      \[$db_password\]\n" ;
    exit 1;
}


my $dbh = DBI->connect($dsn, $db_user, $db_password,
                       {'RaiseError' => 1, AutoCommit => 0,
                        mysql_server_prepare => 1});
die($DBI::errstr) unless $dbh;


my $seq = 0;
if( not $rescan )
{
    my $r = $dbh->selectall_arrayref('SELECT MAX(global_seq) FROM EOSIO_TRANSFERS');
    if( defined($r->[0][0]) )
    {
        $seq = $r->[0][0];
    }
}


my $sth_getirreversible = $dbh->prepare
    ('SELECT val_int FROM EOSIO_VARS WHERE varname=\'last_irreversible_block\'');

my $sth_getactions = $dbh->prepare
    ('SELECT ' .
     ' DATE(block_time) AS bd, block_num, block_time, trx_id, ' .
     ' global_action_seq as seq, jsdata ' .
     'FROM EOSIO_ACTIONS ' .
     'WHERE global_action_seq > ? AND block_num <= ? AND irreversible=1 AND status=0 ' .
     'ORDER BY global_action_seq LIMIT 1000');

my $sth_check_tx = $dbh->prepare
    ('SELECT trx_id FROM EOSIO_TRANSFERS WHERE global_seq=?');

my $sth_add_tx = $dbh->prepare
    ('INSERT INTO EOSIO_TRANSFERS ' .
     '(global_seq, block_num, block_time, trx_id, ' .
     'issuer, currency, amount, tx_from, tx_to, memo, bal_from, bal_to) ' .
     'VALUES(?,?,?,?,?,?,?,?,?,?,?,?)');


my $processing_date = '';
my $rowcnt = 0;

while(1)
{
    $sth_getirreversible->execute();
    my $irrev = $sth_getirreversible->fetchall_arrayref();
    $irrev = $irrev->[0][0];
    
    $sth_getactions->execute($seq, $irrev);

    my $r = $sth_getactions->fetchall_arrayref({});
    my $nrows = scalar(@{$r});
    if( $nrows == 0 )
    {
        last;
    }
    
    foreach my $row (@{$r})
    {
        my $tx = $row->{'trx_id'};
        $seq = $row->{'seq'};
                
        my $action = eval { $json->decode($row->{'jsdata'}) };
        if($@)
        {
            printf("Cannot parse JSON: SEQ=%d ACTION=%s ACTOR=%s\n",
                   $seq, $row->{'action_name'}, $row->{'actor_account'});
            next;
        }
        
        if( $row->{'bd'} ne $processing_date )
        {
            $processing_date = $row->{'bd'};
            print("Processing $processing_date\n");
        }

        my $atrace = $action->{'action_trace'};
        my $data = $atrace->{'act'}{'data'};

        my $state = {'transfers' => []};
        process_action($atrace, $state);
        
        next if scalar(@{$state->{'transfers'}}) == 0;
        
        my %bal;
        foreach my $brow (@{$action->{'currency_balances'}})
        {
            my ($amount, $currency) = split(/\s+/, $brow->{'balance'});
            $bal{$brow->{'account_name'}}{$brow->{'issuer'}}{$currency} = $amount;
        }

        foreach my $transfer (@{$state->{'transfers'}})
        {
            my $gseq = $transfer->{'global_seq'};
            $sth_check_tx->execute($gseq);
            my $results = $sth_check_tx->fetchall_arrayref();
            if( scalar(@{$results}) > 0 )
            {
                if( $results->[0][0] eq $tx )
                {
                    next;
                }
                else
                {
                    die(sprintf('Found TX=%s for global_seq=%d, but expected %s',
                                $tx, $transfer->{'global_seq'}, $results->[0][0]));
                }
            }

            my $issuer = $transfer->{'issuer'};
            my $currency = $transfer->{'currency'};
            my $from = $transfer->{'tx_from'};
            my $to = $transfer->{'tx_to'};

            my $bal_from = defined($from) ? $bal{$from}{$issuer}{$currency} : undef;
            my $bal_to = $bal{$to}{$issuer}{$currency};

            if( not defined($to) or not defined($bal_to) )
            {
                next;
            }

            if( defined($from) and not defined($bal_from) )
            {
                next;
            }
            
            $sth_add_tx->execute
                ($gseq,
                 $row->{'block_num'},
                 $row->{'block_time'},
                 $tx,
                 $issuer,
                 $currency,
                 $transfer->{'amount'}, 
                 $from,
                 $to,
                 $transfer->{'memo'},
                 $bal_from,
                 $bal_to,
                );
            $rowcnt++;
        }
    }        
    
    $dbh->commit();
    print("$rowcnt\n");
}


$dbh->disconnect();

print("Finished\n");


sub process_action
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
                ($aname eq 'transfer' or $aname eq 'issue') )
            {
                if( defined($data->{'quantity'}) and defined($data->{'to'}) )
                {          
                    my ($amount, $currency) = split(/\s+/, $data->{'quantity'});
                    
                    push(@{$state->{'transfers'}}, {
                        'global_seq' => $gseq,
                        'issuer' => $act->{'account'},
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
            process_action($trace, $state);
        }
    }
}
            
            
            
                
    





     

