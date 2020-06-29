=head1 LICENSE

Copyright [1999-2015] Wellcome Trust Sanger Institute and the EMBL-European Bioinformatics Institute
Copyright [2016-2020] EMBL-European Bioinformatics Institute

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

=pod

=head1 NAME

Bio::EnsEMBL::Hive::Utils::HashCollection - A collection object for hashes that can be indexed

=cut

package Bio::EnsEMBL::Hive::Utils::HashCollection;

use strict;
use warnings;

use base ('Bio::EnsEMBL::Hive::Utils::Collection');


sub new {
    my $class           = shift @_;
    my $listref         = shift @_ || [];
    my $unique_attr     = shift @_;

    my $self = bless {}, $class;

    $self->unique_attr($unique_attr);
    $self->{'_lookup'} = {};
    $self->add(@$listref);

    return $self;
}


sub shallow_copy {
    my $self = shift @_;
    return Bio::EnsEMBL::Hive::Utils::HashCollection->new(undef, $self->unique_attr);
}


sub unique_attr {
    my $self    = shift @_;
    if (@_) {
        $self->{'_unique_attr'} = shift @_;
    }
    return $self->{'_unique_attr'};
}


sub get_lookup {
    my $self = shift @_;

    return $self->{'_lookup'};
}


sub listref {
    my $self = shift @_;

    return [$self->list];
}


sub list {
    my $self = shift @_;

    return values %{ $self->get_lookup };
}


sub present {
    my $self        = shift @_;
    my $candidate   = shift @_;

    my $key = $candidate->{$self->unique_attr};
    die sprintf("\$hash->{%s} must be defined,\n", $self->unique_attr) unless defined $key;
    return $self->get_lookup->{$key};
}


sub add {
    my $self = shift @_;

    foreach my $candidate (@_) {
        my $key = $candidate->{$self->unique_attr};
        die sprintf("\$hash->{%s} must be defined,\n", $self->unique_attr) unless defined $key;
        die sprintf("There is already an object with %s=%s in the collection.\n", $self->unique_attr, $key) if $self->get_lookup->{$key};
        $self->get_lookup->{$key} = $candidate;
    }
}


sub forget {
    my $self        = shift @_;
    my $candidate   = shift @_;

    my $key = $candidate->{$self->unique_attr};
    die sprintf("\$hash->{%s} must be defined,\n", $self->unique_attr) unless defined $key;
    delete $self->get_lookup->{$key};
}


sub find_one_by {
    my ($self, @args) = @_;

    if (scalar(@args) == 2) {
        my ($filter_name, $filter_value) = @args;
        if ($filter_name eq $self->unique_attr) {
            if (defined $filter_value) {
                return $self->get_lookup->{$filter_value};
            } else {
                return undef;
            }
        }
    }

    return $self->SUPER::find_one_by(@args);
}


1;
