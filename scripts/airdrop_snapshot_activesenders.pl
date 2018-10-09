use strict;
use warnings;
use JSON;
use Getopt::Long;
use DBI;

$| = 1;

my $dsn = 'DBI:mysql:database=eosio;host=localhost';
my $db_user = 'eosioro';
my $db_password = 'eosioro';

my %currency = ('eosio.token' => {'EOS' => 1});

my $months = 3;
my $snapshot_date;
my $min_balance;
my $min_transfer;

    
my $ok = GetOptions
    ('dsn=s'     => \$dsn,
     'dbuser=s'  => \$db_user,
     'dbpw=s'    => \$db_password,
     'date=s'    => \$snapshot_date,
     'months=i'  => \$months,
     'minbal=f'  => \$min_balance,
     'mintx=f'   => \$min_transfer);

if( not $ok or not $snapshot_date or scalar(@ARGV) > 0 )
{
    print STDERR "Usage: $0 --date=YYYY-MM-DD [options...]\n",
    "The generates an airdrop snapshot of all users who made an EOS transfer in the past months\n",
    "Options:\n",
    "  --dsn=DSN            \[$dsn\]\n",
    "  --dbuser=USER        \[$db_user\]\n",
    "  --dbpw=PASSWORD      \[$db_password\]\n",
    "  --date=YYYY-MM-DD    Stapshot date\n",
    "  --months=N           \[$months\] number of months back\n",
    "  --minbal=X           Minimum EOS balance\n",
    "  --mintx=X            Minimum sum of EOS transfers\n";
    exit 1;
}


my $dbh = DBI->connect($dsn, $db_user, $db_password,
                       {'RaiseError' => 1, AutoCommit => 0});
die($DBI::errstr) unless $dbh;

$dbh->{mysql_use_result} = 1;

my $min_block;
my $max_block;

{
    my $sth = $dbh->prepare
        ('SELECT MIN(block_num) FROM EOSIO_TRANSFERS ' .
         'WHERE block_time BETWEEN ' .
         'DATE_SUB(?, INTERVAL ? MONTH) AND ' .
         'DATE_ADD(DATE_SUB(?, INTERVAL ? MONTH), INTERVAL 2 HOUR)');
    $sth->execute($snapshot_date, $months, $snapshot_date, $months);
    my $r = $sth->fetchall_arrayref();
    die('Cannot find minimum block') unless defined($r->[0][0]);
    $min_block = $r->[0][0];

    $sth = $dbh->prepare
        ('SELECT MAX(block_num) FROM EOSIO_TRANSFERS ' .
         'WHERE block_time BETWEEN ' .
         'DATE_SUB(?, INTERVAL 2 HOUR) AND DATE(?)');
    $sth->execute($snapshot_date, $snapshot_date);
    $r = $sth->fetchall_arrayref();
    die('Cannot find maximum block') unless defined($r->[0][0]);
    $max_block = $r->[0][0];
}


my $sth_gettx = $dbh->prepare
    ('SELECT ' .
     ' issuer, currency, amount, tx_from, tx_to, bal_from, bal_to ' .
     'FROM EOSIO_TRANSFERS ' .
     'WHERE block_num >= ? AND block_num <= ? ORDER BY block_num');


my $sth_getres = $dbh->prepare
    ('SELECT ' .
     ' account_name, cpu_weight, net_weight ' .
     'FROM EOSIO_RESOURCE_BALANCES JOIN EOSIO_ACTIONS ON id=action_id ' .
     'WHERE block_num >= ? AND block_num <= ? ORDER BY block_num');

my %sender;
my %balance;
my %cpustake;
my %netstake;
my %totaltx;

my $curr_block = $min_block;
while( $curr_block < $max_block )
{
    my $upper_block = $curr_block + 7200;
    $upper_block = $max_block if $upper_block > $max_block;

    $sth_gettx->execute($curr_block, $upper_block);    
    while(my $row = $sth_gettx->fetchrow_hashref('NAME_lc') )
    {
        next unless $currency{$row->{'issuer'}}{$row->{'currency'}};
        
        if( defined($row->{'tx_from'}) )
        {
            $sender{$row->{'tx_from'}} = 1;
            $balance{$row->{'tx_from'}} = $row->{'bal_from'};
            $totaltx{$row->{'tx_from'}} += $row->{'amount'};
        }
        
        $balance{$row->{'tx_to'}} = $row->{'bal_to'};
        $totaltx{$row->{'tx_to'}} += $row->{'amount'};
    }

    $sth_getres->execute($curr_block, $upper_block);
    while(my $row = $sth_getres->fetchrow_hashref('NAME_lc') )
    {
        $cpustake{$row->{'account_name'}} = $row->{'cpu_weight'};
        $netstake{$row->{'account_name'}} = $row->{'net_weight'};
    }
    
    $curr_block = $upper_block + 1;
    print STDERR '.';
}


$dbh->disconnect();

foreach my $account (sort keys %sender)
{
    my $bal = $balance{$account} + $cpustake{$account} + $netstake{$account};
    my $ok = 1;
    if( defined($min_balance) or defined($min_transfer) )
    {
        $ok = 0;
        if( defined($min_balance) and $bal >= $min_balance )
        {
            $ok = 1;
        }

        if( defined($min_transfer) and $totaltx{$account} >= $min_transfer )
        {
            $ok = 1;
        }
    }

    if( $ok )
    {
        print $account, ',', $bal, "\n";
    }
}


