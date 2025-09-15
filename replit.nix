{ pkgs }: {
	deps = [
		pkgs.nodejs-18_x
    pkgs.yarn
    pkgs.replitPackages.jest
    pkgs.chromium
	];
}