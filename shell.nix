{ pkgs ? import (import ./pkgs.nix) {}}:
with pkgs;
let p = haskellPackages.callCabal2nix "breakers" ./. {};
in p.env
