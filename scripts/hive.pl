#!/usr/bin/env perl

use strict;
use warnings;

    # Finding out own path in order to reference own components (including own modules):
use Cwd            ();
use File::Basename ();
BEGIN {
    $ENV{'EHIVE_ROOT_DIR'} ||= File::Basename::dirname( File::Basename::dirname( Cwd::realpath($0) ) );
    unshift @INC, $ENV{'EHIVE_ROOT_DIR'}.'/modules';
}

use Data::Dumper;

use Bio::EnsEMBL::Hive::Utils ('find_submodules');

#use Getopt::Long qw(:config no_auto_abbrev);

my %blacklist = map {$_=>1} qw(Bio::EnsEMBL::Hive::Scripts::BaseScript Bio::EnsEMBL::Hive::Scripts::RunWorker);

my %legacy_scripts = (
    'seed_pipeline.pl'  => 'Seed',
    'tweak_pipeline.pl' => 'Tweak',
    'init_pipeline.pl'  => 'InitPipeline',
);

my %recognized_actions = (
    # Aliases
    'version'   => 'Versions',
    'init'      => 'InitPipeline',
);

foreach my $s (@{ find_submodules('Bio::EnsEMBL::Hive::Scripts') }) {
    next if $blacklist{$s};
    $s =~ s/Bio::EnsEMBL::Hive::Scripts:://;
    $recognized_actions{lc $s} = $s;
}

foreach my $s (keys %legacy_scripts) {
    my $a = $legacy_scripts{$s};
    $s =~ s/\.pl$//;
    # Topup the hash with the script name
    $recognized_actions{lc $s} = $a;
}

#print Dumper(\%recognized_actions);

## Let's try to find the name of the action
my $action;

my $this_script_name = File::Basename::basename(lc $0);
if ($this_script_name =~ /^hive(\.pl)?/) {
    unless (@ARGV) {
        die "Syntax error. Use $0 <action> <parameters>";
    }
    $action = shift @ARGV;
    $action = $recognized_actions{lc $action} || die "Unrecognized action '$action'. Use one of: ".join(", ", keys %recognized_actions)."\n";
} else {
    $action = $legacy_scripts{$this_script_name} or die "Unrecognized script name '$this_script_name'";
}

my $module_name = 'Bio::EnsEMBL::Hive::Scripts::'.(ucfirst $action);

eval "require $module_name";
if ($@) {
    die "Cannot load the module $module_name for action '$action': $@";
} else {
    my $script_object = $module_name->new();
    $script_object->main();
}

