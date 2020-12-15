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

Bio::EnsEMBL::Hive::Utils::IndexedCollection - A collection object that is indexed

=cut

package Bio::EnsEMBL::Hive::Utils::IndexedCollection;

use strict;
use warnings;

use base ('Bio::EnsEMBL::Hive::Utils::Collection');


sub new {
    my $class           = shift @_;
    my $listref         = shift @_ || [];
    my $lookup_field    = shift @_ || [];

    my $self = bless {}, (ref($class) || $class);

    $self->listref($listref);
    $self->lookup_field($lookup_field);

    return $self;
}


sub shallow_copy {
    my $self = shift @_;
    return Bio::EnsEMBL::Hive::Utils::Collection->new(undef, $self->lookup_field);
}


sub lookup_field {
    my $self    = shift @_;
    if (@_) {
        $self->{'_lookup_field'} = shift @_;
    }
    return $self->{'_lookup_field'};
}


sub build_lookup {
    my $self    = shift @_;
    $self->{'_lookup'} = {};
    $self->_add_element_to_lookup($_) for $self->list;
}


sub is_lookup_built {
    my $self    = shift @_;
    return exists $self->{'_lookup'};
}


sub get_lookup {
    my $self    = shift @_;
    $self->build_lookup() unless $self->is_lookup_built();
    return $self->{'_lookup'};
}


sub invalidate_lookup {
    my $self = shift @_;
    delete $self->{'_lookup'};
}


sub _add_element_to_lookup {
    my $self    = shift @_;
    my $element = shift @_;
    my $field   = $self->lookup_field;
    my $lookup  = $self->get_lookup;
    my $value   = $element->$field();
    if (defined $value) {
        if ($lookup->{$value}) {
            die "'$value' is not unique\n";
        } else {
            $lookup->{$value} = $element;
        }
    }
}


sub add {
    my $self = shift @_;

    $self->SUPER::add(@_);

    $self->_add_element_to_lookup($_) for @_;
}


sub forget {
    my $self        = shift @_;
    my $candidate   = shift @_;

    my $listref = $self->listref;

    my $object_not_found;
    # Don't build the lookup if it doesn't exist since we'll have to traverse the list anyway
    if ($self->is_lookup_built()) {
        my $lookup = $self->get_lookup();
        my $field = $self->lookup_field();
        my $value = $candidate->$field();
        delete $lookup->{$value};
    }
    $self->SUPER::forget($candidate);
}


sub find_one_by {
    my $self = shift @_;
    my %method_to_filter_value = @_;

    # If there is just one filter and we have indexed the column and we use
    # a value to filter the objects, then yes we can speed this up !
    if (scalar(keys %method_to_filter_value) == 1) {
        my ($filter_name, $filter_value) = (%method_to_filter_value);
        if (($filter_name eq $self->lookup_field) && defined($filter_value) && (ref($filter_value) ne 'CODE')) {
            return $self->get_lookup()->{$filter_value};
        }
    }

    return $self->SUPER::find_one_by(@_);
}

1;
