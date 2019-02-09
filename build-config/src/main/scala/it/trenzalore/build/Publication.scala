package it.trenzalore.build

sealed trait Publication
case object Assembly extends Publication
case object None extends Publication
