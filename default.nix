with import <nixpkgs> { };

pkgs.stdenv.mkDerivation rec {
  name = "pgquarrel";
  src = ./.;
  buildInputs = [
    cmake
    pkgconfig
    postgresql96
    openssl
  ];
}
