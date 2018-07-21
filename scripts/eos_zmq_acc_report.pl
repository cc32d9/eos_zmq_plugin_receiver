use strict;
use warnings;
use JSON;
use Getopt::Long;
use DBI;
use Excel::Writer::XLSX; 


my $account;
my $xlsx_out;
my $start;
my $months = 3;

my $dsn = 'DBI:mysql:database=eosio;host=localhost';
my $db_user = 'eosio';
my $db_password = 'guugh3Ei';

my $json = JSON->new;

my $ok = GetOptions
    ('acc=s'     => \$account,
     'out=s'     => \$xlsx_out,
     'start=s'   => \$start,
     'months=i'  => \$months,
     'dsn=s'     => \$dsn,
     'dbuser=s'  => \$db_user,
     'dbpw=s'    => \$db_password);


if( not $ok or scalar(@ARGV) > 0 or not defined($account) or
    not defined($xlsx_out) or not defined($start) )
{
    print STDERR "Usage: $0 --acc=NAME --out=FILE.xlsx --start=YYYY-MM-DD [options...]\n",
    "The utility generates accounting report from EOS SQL database\n",
    "Options:\n",
    "  --acc=NAME           eosio account name\n",
    "  --out=FILE.xlsx      output file\n",
    "  --start=YYYY-MM-DD   start date (also understood YYYYQn)\n",
    "  --months=N           \[$months\] number of months to report\n",
    "  --dsn=DSN            \[$dsn\]\n",
    "  --dbuser=USER        \[$db_user\]\n",
    "  --dbpw=PASSWORD      \[$db_password\]\n" ;
    exit 1;
}

my ($year, $month, $day);
if( $start =~ /^(\d{4})-(\d{2})-(\d{2})$/ )
{
    ($year, $month, $day) = ($1, $2, $3);
}
elsif( $start =~ /^(\d{4})Q(\d)$/ )
{
    $year = $1;
    $month = 1 + ($2 - 1)*3;
    $day = '01';
}
else
{
    die("invalid start format: $start\n");
}


my $start_date = sprintf('%.4d-%.2d-%.2d', $year, $month, $day);

my @columns =
    ('block_time', 'seq', 'actor', 'action', 'recipient',
     'from', 'to', 'quantity', 'memo',
     'issuer', 'currency', 'balance', 'cpu_stake', 'net_stake',
     'ram_quota', 'ram_usage', 'trx_id');

my @transfer_columns =
    ('from', 'to', 'quantity', 'memo');

my %width =
    (
     'block_time' => 22,
     'quantity' => 22,
     'memo' => 40,
     'currency' => 10,
     'balance' => 12,
     'cpu_stake' => 12,
     'net_stake' => 12,
     'ram_quota' => 12,
     'ram_usage' => 12,
     'trx_id' => 80,
    );

my %col_money =
    ('amount' => 1,
     'cpu_stake' => 1,
     'net_stake' => 1);

my %col_integer =
    ('ram_quota' => 1,
     'ram_usage' => 1);
     
my $dbh = DBI->connect($dsn, $db_user, $db_password,
                       {'RaiseError' => 1, AutoCommit => 0,
                        mysql_server_prepare => 1});
die($DBI::errstr) unless $dbh;

my $workbook = Excel::Writer::XLSX->new($xlsx_out) or die($!);
my $worksheet = $workbook->add_worksheet($start_date . ' ' . $months . ' months');

my $c_tblheader = $workbook->set_custom_color(40, '#003366');

my $f_tblheader = $workbook->add_format
    ( bold => 1,
      bottom => 1,
      align => 'center',
      bg_color => $c_tblheader,
      color => 'white' ); 

my $f_money = $workbook->add_format(num_format => '0.0000');

my $col = 0;
my $row = 0;

foreach my $colname (@columns)
{
    my $w = $width{$colname};
    $w = 16 unless defined($w);

    $worksheet->set_column($col, $col, $w);
    $worksheet->write($row, $col, $colname, $f_tblheader);
    $col++;
}


my $sth = $dbh->prepare
    ('SELECT ' .
     ' block_time, EOSIO_ACTIONS.global_action_seq AS seq, actor_account AS actor, ' .
     ' action_name AS action, recipient_account AS recipient, ' .
     ' issuer, currency, amount AS balance, ' .
     ' cpu_weight AS cpu_stake, net_weight AS net_stake, ' .
     ' ram_quota, ram_usage, trx_id, jsdata ' .
     'FROM EOSIO_ACTIONS ' .
     'JOIN ' .
     'EOSIO_RESOURCE_BALANCES ON ' .
     '  EOSIO_RESOURCE_BALANCES.global_action_seq=EOSIO_ACTIONS.global_action_seq ' .
     'JOIN ' .
     'EOSIO_CURRENCY_BALANCES ON ' .
     '  EOSIO_CURRENCY_BALANCES.global_action_seq=EOSIO_ACTIONS.global_action_seq AND ' .
     '  EOSIO_CURRENCY_BALANCES.account_name=EOSIO_RESOURCE_BALANCES.account_name ' .
     'WHERE EOSIO_CURRENCY_BALANCES.account_name=? AND ' .
     '  block_time BETWEEN ? AND DATE_ADD(?, INTERVAL ? MONTH) ' .
     'ORDER BY EOSIO_ACTIONS.global_action_seq');


$sth->execute($account, $start_date, $start_date, $months);

while( my $r = $sth->fetchrow_hashref('NAME_lc') )
{
    my $action = $json->decode($r->{'jsdata'});
    my $atrace = $action->{'action_trace'};

    foreach my $colname (@transfer_columns)
    {
        $r->{$colname} = '';
    }

    my $data = $atrace->{'act'}{'data'};

    if( $r->{'action'} eq 'transfer' )
    {
        foreach my $colname (@transfer_columns)
        {
            if( defined($data->{$colname}) )
            {
                $r->{$colname} = $data->{$colname};
            }
        }
    }
    if( $r->{'action'} eq 'issue' )
    {
        foreach my $colname ('to', 'quantity', 'memo')
        {
            if( defined($data->{$colname}) )
            {
                $r->{$colname} = $data->{$colname};
            }
        }
    }
    elsif($r->{'actor'} eq 'eosio' )
    {
        if( $r->{'action'} eq 'buyrambytes' )
        {
            $r->{'from'} = $data->{'payer'};
            $r->{'to'} = $data->{'receiver'};
            $r->{'quantity'} = $data->{'bytes'} . ' bytes';
        }
        if( $r->{'action'} eq 'buyram' )
        {
            $r->{'from'} = $data->{'payer'};
            $r->{'to'} = $data->{'receiver'};
            $r->{'quantity'} = $data->{'quant'};
        }
        elsif( $r->{'action'} eq 'delegatebw' )
        {
            $r->{'from'} = $data->{'from'};
            $r->{'to'} = $data->{'receiver'};
            $r->{'quantity'} = $data->{'stake_cpu_quantity'} . ' + '
                . $data->{'stake_net_quantity'} ;
        }
        elsif( $r->{'action'} eq 'refund' )
        {
            my $traces = $atrace->{'inline_traces'};
            foreach my $tr (@{$traces})
            {
                if( $tr->{'act'}{'name'} eq 'transfer' )
                {
                    foreach my $colname (@transfer_columns)
                    {
                        $r->{$colname} = $tr->{'act'}{'data'}{$colname};
                    }
                    last;
                }
            }
        }
    }
        
    $row++;
    $col = 0;
    
    foreach my $colname (@columns)
    {
        if( $col_money{$colname} )
        {
            $worksheet->write_number($row, $col, $r->{$colname}, $f_money);
        }
        elsif( $col_integer{$colname} )
        {
            $worksheet->write_number($row, $col, $r->{$colname});
        }
        else
        {
            $worksheet->write_string($row, $col, $r->{$colname});
        }
        $col++;
    }
}

$dbh->disconnect();
$workbook->close();


