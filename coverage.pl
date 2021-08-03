#!/usr/bin/env perl
use strict;
use warnings;
$, = "\n";
$\ = "\n";

my %flowids = (
  years_at_sea_resolver => ['Wrong number of "years at sea" entries (or bad separator)',
                            'Non-float argument in "years at sea"',
                            'Autoresolved',
                            'Unanimous',
                            'Unresolvable (both sides)',
                            'Unresolvable (navy side)',
                            'Unresolvable (merchant side)'],
  string_resolver => ['port sailed out of in volume 1 (later)',
                      'port sailed out of in volume 1 (first)',
                      'Did not pass threshold',
                      'Later autoresolve',
                      'First autoresolve',
                      'Unambiguous'],
  date_resolver => ['Surprising input',
                    'Unparseable',
                    'Autoresolved',
                    'Unanimous',
                    'Unresolvable',
                    'Classified blank'],
  number_resolver => ['Surprising input',
                      'Non-float input',
                      'Non-integer input',
                      'Autoresolved',
                      'Unanimous',
                      'Unresolvable'],
  drop_resolver => ['Autoresolved',
                    'Unanimous',
                    'Unresolvable']
);
my %unseen;
while(my ($prefix, $postfixen) = each %flowids) {
  foreach my $postfix (@$postfixen) {
    $unseen{"${prefix} ${postfix}"} = '*';
  }
}

my @unknown = ();
my @identifiers = keys %unseen;
my @output = grep(/^FR: /, `./aggregate.py -f @ARGV`);
foreach (@output) {
  chomp;
  $_ = substr($_, 4);
}
LINE: foreach my $line (@output) {
  foreach my $identifier (@identifiers) {
    if($line eq $identifier) {
      delete $unseen{$identifier};
      next LINE;
    }
  }
  push @unknown, $line;
}

my $errcode = 0;
if(%unseen) {
  print 'The following paths were not covered by the inputs:';
  print sort keys %unseen;
  $errcode += 1;
}
if(@unknown) {
  print '' if $errcode;
  print 'The following FR strings were unknown:';
  print sort @unknown;
  $errcode += 2;
}
exit($errcode) if($errcode);
print 'All paths covered by inputs';
