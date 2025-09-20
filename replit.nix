{ pkgs }: {
	deps = [
   pkgs.unzip
		pkgs.nodejs-18_x
    pkgs.yarn
    pkgs.replitPackages.jest
    pkgs.chromium
	];
}