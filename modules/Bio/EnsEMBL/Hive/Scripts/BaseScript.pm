=head1 LICENSE

Copyright [1999-2015] Wellcome Trust Sanger Institute and the EMBL-European Bioinformatics Institute
Copyright [2016-2017] EMBL-European Bioinformatics Institute

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

=cut


package Bio::EnsEMBL::Hive::Scripts::BaseScript;

use strict;
use warnings;

use Data::Dumper;
use Getopt::Long qw(:config no_auto_abbrev);
use Pod::Usage;

use Bio::EnsEMBL::Hive::HivePipeline;

use base ('Bio::EnsEMBL::Hive::Params');

sub add_connection_command_line_options { 0 };
sub must_use_all_command_line_options { 1 };

my @connection_command_line_options = (
    'url=s'                        => 'url',
    'reg_conf|regfile|reg_file=s'  => 'reg_conf',
    'reg_type=s'                   => 'reg_type',
    'reg_alias|regname|reg_name=s' => 'reg_alias',
    'nosqlvc=i'                    => 'nosqlvc',      # using "=i" instead of "!" for consistency with scripts where it is a propagated option
);

sub command_line_options {
    return [];
}

sub parse_options {
    my $self = shift;

    my $options = $self->command_line_options;
    push @$options, @connection_command_line_options if $self->add_connection_command_line_options();
    return unless @$options;

    my @getopt_params;
    my %options_as_hash = @$options;
    my %param_hash;
    foreach my $opt (keys %options_as_hash) {
        my $param_name;
        my $param_init_value;

        # Default value and parameter name
        if (ref($options_as_hash{$opt}) eq 'ARRAY') {
            $param_name = $options_as_hash{$opt}->[0];
            $param_init_value = $options_as_hash{$opt}->[1];
        } elsif (ref($options_as_hash{$opt}) eq 'CODE') {
            $param_init_value = $options_as_hash{$opt};
        } else {
            $param_name = $options_as_hash{$opt};
            if ($opt =~ /\@/) {
                $param_init_value = [];
            }
        }

        if ($param_name) {
            $param_hash{$param_name} = $param_init_value;
            push @getopt_params, $opt => (ref $param_init_value ? $param_init_value : \$param_hash{$param_name});
        } else {
            push @getopt_params, $opt => $param_init_value;
        }
    }

    #print Dumper(\@getopt_params, \@ARGV, $self->{_param_hash}, \%param_hash);

    my $help;
    push @getopt_params, 'h|help!' => \$help;

    Getopt::Long::Configure('pass_through') unless $self->must_use_all_command_line_options();
    GetOptions( @getopt_params ) or die "Error in command line arguments\n";

    #print Dumper(\@ARGV, $self->{_param_hash}, \%param_hash, $help);

    if ($self->must_use_all_command_line_options && scalar(@ARGV)) {
        die "ERROR: There are invalid arguments on the command-line: ". join(" ", @ARGV). "\n";
    }

    if ($help) { $self->script_usage(0); }

    $self->param_init(\%param_hash);

    if ($self->add_connection_command_line_options()) {
        if($self->param('url') or $self->param('reg_alias')) {

            # Perform environment variable substitution separately with and without curly braces.
            #       Fixme: Perl 5.10 has a cute new "branch reset" (?|pattern)
            #              that would allow to merge the two substitutions below into a nice one-liner.
            #              But people around may still be using Perl 5.8, so let's wait a bit.
            #
            # Make sure expressions stay as they were if we were unable to substitute them.
            my $url = $self->param('url');
            if($url) {
                $url =~ s/\$(\{(\w+)\})/defined($ENV{$2})?"$ENV{$2}":"\$$1"/eg;
                $url =~ s/\$((\w+))/defined($ENV{$2})?"$ENV{$2}":"\$$1"/eg;
            }

            $self->param('pipeline', Bio::EnsEMBL::Hive::HivePipeline->new(
                    -url                            => $url,
                    -reg_conf                       => $self->param('reg_conf'),
                    -reg_type                       => $self->param('reg_type'),
                    -reg_alias                      => $self->param('reg_alias'),
                    -no_sql_schema_version_check    => $self->param('nosqlvc'),
            ) );
        } else {
            die "\nERROR: Connection parameters (url or reg_conf+reg_alias) need to be specified\n";
        }
    }
}

sub main {
    my $self = shift;
    $self->parse_options();
    $self->run();
}


=head2 script_usage

    Description: This function takes one argument (return value).
                 It uses Pod::Usage to display the POD of the current script module and exits with the return value given.

    Callers    : scripts

=cut

sub script_usage {
    my ($self, $retvalue) = @_;

    my $path = ref($self).'.pm';
    $path =~ s/::/\//g;
    local $0 = $INC{$path};
    pod2usage(
        -exitval => $retvalue,
        -verbose => 2,
    );
}


1;
