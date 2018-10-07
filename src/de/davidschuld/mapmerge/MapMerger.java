package de.davidschuld.mapmerge;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Stack;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MapMerger {

	private static final String SE = "_SE";

	private static final String SW = "_SW";

	private static final String NE = "_NE";

	private static final String NW = "_NW";

	private static final String S = "_S";

	private static final String N = "_N";

	private static final CharSequence EXTENSION_PNG = ".png";

	private static final String TMP_DIR = "tmp";

	private final int poolSize = 4;

	private final Stack<Integer> fourWayStack = new Stack();

	private final ThreadPoolExecutor exe = new ScheduledThreadPoolExecutor(poolSize);

	public static void main(String[] args) throws IOException, InterruptedException {

		String baseDir = args[0];
		MapMerger merger = new MapMerger();
		Integer baseImageNum = null;
		if (args.length > 1) {

			for (int i = 1; i < args.length; i++) {
				String option = args[i];
				if (option.equals("-debug")) {
					merger.debug = true;
				} else {
					baseImageNum = Integer.parseInt(option);
				}
			}
		}

		System.out.println("Starting merge in " + baseDir);

		Date startDate = new Date();

		if (baseImageNum != null) {
			merger.run(baseDir, baseImageNum);
		} else {
			merger.run(baseDir);
		}

		merger.fourWayStack.stream().forEach(num -> merger.fourWayMerge(num));

		merger.waitUntilFinished();

		Date endDate = new Date();
		long time = endDate.getTime() - startDate.getTime();

		System.out.println("Elapsed time:" + (time / 1000));

		System.exit(0);

	}

	private void waitUntilFinished() throws InterruptedException {
		while (exe.getActiveCount() + exe.getQueue().size() > 0) {
			Thread.sleep(1000);
			System.out.println("Remaining tasks: " + (exe.getActiveCount() + exe.getQueue().size()));

		}

	}

	private void run(String baseDir, Integer baseImageNum) throws IOException, InterruptedException {
		this.baseDir = baseDir + (baseDir.endsWith(File.separator) ? "" : File.separator);
		horizontalMerge(baseDir, baseImageNum);
		verticalMerge(baseDir, baseImageNum);

		System.out.println("Done with merging baseImage " + baseImageNum);

		if (Files.exists(Paths.get(baseDir, String.valueOf(baseImageNum)))
				&& Files.exists(Paths.get(baseDir, String.valueOf(baseImageNum + 1)))
				&& Files.exists(Paths.get(baseDir, String.valueOf(baseImageNum + 6)))
				&& Files.exists(Paths.get(baseDir, String.valueOf(baseImageNum + 7)))) {
			fourWayStack.push(baseImageNum);
		}

	}

	private String baseDir;

	private boolean debug = false;

	public void run(String baseDir) throws IOException, InterruptedException {

		this.baseDir = baseDir + (baseDir.endsWith(File.separator) ? "" : File.separator);
		runMerge();

	}

	private void runMerge() throws IOException, InterruptedException {

		// first dir has left margin
		// last dir has right margin
		// first file in each dir has top margin
		// last file in each dir has top margin

		int i = 1;
		while (i < 36) {
			while (i % 6 != 0) {
				run(baseDir, i);

				i++;
			}

			i++;
		}

	}

	private void fourWayMerge(Integer baseImageNum) {
		// if baseImages exist: i, i+1, i+6, i+7
		// four images
		// - northwest last image in last x folder
		// - northeast last image in first x folder
		// - southwest first image in last x folder
		// - southeast first image in first or second x folder

		try {
			Files.list(Paths.get(baseDir, String.valueOf(baseImageNum))).filter(Files::isDirectory).forEach(zoomDir -> {
				Path northWestTile = getLastEntry(getLastEntry(zoomDir, Files::isDirectory), var -> true);
				Path northEastTile = Paths.get(northWestTile.toString().replace(separatedImageDir(baseImageNum),
						separatedImageDir(baseImageNum + 1)));
				Path southWestTile = Paths.get(northWestTile.toString().replace(separatedImageDir(baseImageNum),
						separatedImageDir(baseImageNum + 6)));
				Path southEastTile = Paths.get(northWestTile.toString().replace(separatedImageDir(baseImageNum),
						separatedImageDir(baseImageNum + 7)));

				System.out.println("Four way merge with:");
				System.out.println(northWestTile);
				if (!Files.exists(northWestTile) || !Files.exists(northEastTile) || !Files.exists(southWestTile)
						|| !Files.exists(southEastTile)) {
					System.out.println("Tile does not exist! Skipping directory");
					return;
				}

				runFourWayMerge(baseImageNum, northWestTile, northEastTile, southWestTile, southEastTile);
			});
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

	}

	private Path getLastEntry(Path dir, Predicate<? super Path> filter) {

		List<? extends Path> tiles = listFilesOrThrow(dir).filter(filter).collect(Collectors.toList());

		Collections.sort(tiles, pathComparator());

		return tiles.get(tiles.size() - 1);

	}

	private Comparator<Path> pathComparator() {
		return (p1, p2) -> {
			int x1 = Integer.parseInt(
					p1.toString().substring(p1.toString().lastIndexOf(File.separator) + 1).replace(EXTENSION_PNG, ""));
			int x2 = Integer.parseInt(
					p2.toString().substring(p2.toString().lastIndexOf(File.separator) + 1).replace(EXTENSION_PNG, ""));
			return x1 - x2;
		};
	}

	/*
	 * In the horizontal merge, the easternmost images of the western base image
	 * have to be merged with the westernmost images in the eastern base image.
	 * 
	 * In theory, the highest x folder in the wester image should correspond to the
	 * lowest x folder in the eastern image, and all tiles in these folder have to
	 * be merged. However actually, since the base images are slightly rotated, the
	 * 2 highest x folders in the western image match with the 2 lowest x folders in
	 * the eastern image.
	 * 
	 * The folder structure is /baseImage/zoom/x/y.png, i.e. in each zoom folder,
	 * the 2 highest x directories in west have to be enumerated and its tiles have
	 * to be matched with the tiles in the 2 lowest x dirs in east.
	 */
	private void horizontalMerge(String basedir, int westernBaseImage) throws IOException, InterruptedException {

		int easternBaseImage = westernBaseImage + 1;
		if (!Files.exists(Paths.get(basedir, String.valueOf(westernBaseImage)))
				|| !Files.exists(Paths.get(basedir, String.valueOf(easternBaseImage)))) {
			return;
		}
		System.out.println("Horizontal Merge " + westernBaseImage + "," + easternBaseImage);
		List<? extends Path> zoomDirs = Files.list(Paths.get(basedir, String.valueOf(westernBaseImage)))
				.filter(Files::isDirectory).collect(Collectors.toList());

		zoomDirs.forEach(zoomDir -> {
			List<? extends Path> xDirs = listFilesOrThrow(zoomDir).filter(Files::isDirectory)
					.collect(Collectors.toList());

			Collections.sort(xDirs, pathComparator());

			try {
				if (xDirs.size() > 1) {
					horizontalMergeOnZoomLevel(westernBaseImage, easternBaseImage, xDirs.get(xDirs.size() - 2));
				}
				horizontalMergeOnZoomLevel(westernBaseImage, easternBaseImage, xDirs.get(xDirs.size() - 1));
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		});

	}

	private void horizontalMergeOnZoomLevel(int westernBaseImage, int easternBaseImage, Path xDir)
			throws IOException, InterruptedException {

		System.out.println("Checking " + removeBaseDir(xDir));
		ArrayList<Path> tileList = new ArrayList<>();
		Files.newDirectoryStream(xDir).forEach(tile -> tileList.add(tile));

		for (Path tile : tileList) {

			Path easternTile = Paths.get(
					tile.toString().replace(separatedImageDir(westernBaseImage), separatedImageDir(easternBaseImage)));

			if (Files.exists(easternTile)) {

				mergeFilesHorizontally(westernBaseImage, easternBaseImage, easternTile);

			}
		}

	}

	private void mergeFilesHorizontally(int westernBaseImage, int easternBaseImageImage, Path tile)
			throws IOException, InterruptedException {
		String[] split = removeBaseDir(tile).split("[" + File.separator + "\\.]");
		String zoom = split[1];
		String dirNum = split[2];
		String tileNumber = split[3];

		createOutputDir(zoom, dirNum);

		runImageMagickMergeHorizontally(westernBaseImage, easternBaseImageImage, zoom, dirNum, tileNumber);
	}

	private void verticalMerge(String basedir, int northernBaseImage) throws IOException {

		int southernBaseImage = northernBaseImage + 6;
		if (!Files.exists(Paths.get(basedir, String.valueOf(northernBaseImage)))
				|| !Files.exists(Paths.get(basedir, String.valueOf(southernBaseImage)))) {
			return;
		}
		System.out.println("Vertical Merge " + northernBaseImage + "," + southernBaseImage);

		Files.list(Paths.get(basedir, String.valueOf(northernBaseImage))).filter(Files::isDirectory)
				.flatMap(p -> listFilesOrThrow(p))
				.forEach(verticalMergeOnZoomLevel(northernBaseImage, southernBaseImage));

	}

	/*
	 * In the vertical merge, the southernmost images of the northern base image
	 * have to be merged with the northernmost images in the southern base image.
	 * The folder structure is /baseImage/zoom/x/y.png, i.e. in each zoom folder,
	 * the x directories have to be enumerated and in each x dir the southernmost
	 * tile (i.e. the tile with the highest number) has to be identified. Then, the
	 * corresponding tile (same zoom/x/y values) has to be retrieved in the southern
	 * base image.
	 */
	private Consumer<? super Path> verticalMergeOnZoomLevel(int northernBaseImage, int southernBaseImage) {
		return p -> {
			System.out.println("Checking " + removeBaseDir(p));
			try {
				ArrayList<Path> fileList = new ArrayList<>();
				Files.newDirectoryStream(p).forEach(file -> fileList.add(file));
				Collections.sort(fileList);

				Path northernTile = fileList.get(fileList.size() - 1);
				Path southernTile = Paths.get(northernTile.toString().replace(separatedImageDir(northernBaseImage),
						separatedImageDir(southernBaseImage)));
				if (Files.exists(southernTile)) {

					mergeFilesVertically(northernBaseImage, southernBaseImage, northernTile);

				}

			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		};
	}

	private void mergeFilesVertically(int northernBaseImage, int southernBaseImageImage, Path tile)
			throws IOException, InterruptedException {
		String[] split = removeBaseDir(tile).split("[" + File.separator + "\\.]");
		String zoom = split[1];
		String dirNum = split[2];
		String tileNumber = split[3];

		createOutputDir(zoom, dirNum);

		runImageMagickMergeVertically(northernBaseImage, southernBaseImageImage, zoom, dirNum, tileNumber);
	}

	private void createOutputDir(String zoom, String dirNum) {
		String outputDir = baseDir + File.separator + TMP_DIR + File.separator + zoom + File.separator + dirNum
				+ File.separator;
		File directory = new File(outputDir);
		if (!directory.exists()) {
			directory.mkdirs();
		}
	}

	private void runImageMagickMergeVertically(int northernBaseImage, int southernBaseImageImage, String zoom,
			String dirNum, String tileNumber) throws IOException, InterruptedException {

		exe.execute(() -> {
			executeSystemCommand(constructOneSidedTrimCommand(String.valueOf(northernBaseImage), zoom, dirNum,
					tileNumber, "North", "0x2"));
			executeSystemCommand(constructOneSidedTrimCommand(String.valueOf(southernBaseImageImage), zoom, dirNum,
					tileNumber, "South", "0x2"));
			
			executeSystemCommand(constructChopPixelVerticalCommand(createTmpImagePath(zoom, dirNum, tileNumber, "North"), "South"));
			executeSystemCommand(constructChopPixelVerticalCommand(createTmpImagePath(zoom, dirNum, tileNumber, "South"), "North"));
			
			executeSystemCommand(constructAppendVerticallyCommand(zoom, dirNum, tileNumber));
			executeSystemCommand("rm " + createTmpImagePath(zoom, dirNum, tileNumber, "North"));
			executeSystemCommand("rm " + createTmpImagePath(zoom, dirNum, tileNumber, "South"));
		});
	}

	private void runImageMagickMergeHorizontally(int westernBaseImage, int easternBaseImageImage, String zoom,
			String dirNum, String tileNumber) throws IOException, InterruptedException {

		exe.execute(() -> {
			executeSystemCommand(constructOneSidedTrimCommand(String.valueOf(westernBaseImage), zoom, dirNum,
					tileNumber, "West", "2x0"));
			executeSystemCommand(constructOneSidedTrimCommand(String.valueOf(easternBaseImageImage), zoom, dirNum,
					tileNumber, "East", "2x0"));

			
			executeSystemCommand(constructChopPixelHorizontalCommand(createTmpImagePath(zoom, dirNum, tileNumber, "East"), "West"));
			executeSystemCommand(constructChopPixelHorizontalCommand(createTmpImagePath(zoom, dirNum, tileNumber, "West"), "East"));
			
			executeSystemCommand(constructAppendHorizontallyCommand(zoom, dirNum, tileNumber));
			executeSystemCommand("rm " + createTmpImagePath(zoom, dirNum, tileNumber, "West"));
			executeSystemCommand("rm " + createTmpImagePath(zoom, dirNum, tileNumber, "East"));
		});
	}

	private String removeBaseDir(Path p) {
		return p.toString().replace(baseDir, "");
	}

	private String separatedImageDir(int image) {
		return File.separatorChar + String.valueOf(image) + File.separatorChar;
	}

	private String separatedImageDir(String id) {
		return File.separatorChar + id + File.separatorChar;
	}

	private Stream<? extends Path> listFilesOrThrow(Path p) {
		try {
			return Files.list(p);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private void logDebug(String message) {
		if (debug) {
			System.out.println(message);
		}
	}

	private void executeSystemCommand(String command) {
		logDebug("Running " + command);

		execute(command);
	}

	private void runFourWayMerge(Integer baseImageNum, Path northWestTile, Path northEastTile, Path southWestTile,
			Path southEastTile) {
		Function<String, String> getImagePath = (direction -> northWestTile.toString()
				.replace(separatedImageDir(baseImageNum), separatedImageDir(TMP_DIR))
				.replace(EXTENSION_PNG, direction + EXTENSION_PNG));

		exe.execute(() -> {
			logDebug("Merging " + northEastTile.toString());

			executeSystemCommand(constructSimpleTrimCommand(northWestTile, baseImageNum, NW));
			executeSystemCommand(constructSimpleTrimCommand(northEastTile, baseImageNum + 1, NE));
			executeSystemCommand(constructSimpleTrimCommand(southWestTile, baseImageNum + 6, SW));
			executeSystemCommand(constructSimpleTrimCommand(southEastTile, baseImageNum + 7, SE));


			executeSystemCommand(constructChopPixelHorizontalCommand(getImagePath.apply(NW), "East"));
			executeSystemCommand(constructChopPixelHorizontalCommand(getImagePath.apply(NE), "West"));
			executeSystemCommand(constructChopPixelHorizontalCommand(getImagePath.apply(SW), "East"));
			executeSystemCommand(constructChopPixelHorizontalCommand(getImagePath.apply(SE), "West"));
			
			executeSystemCommand(
					constructAppendHCommand(getImagePath.apply(NW), getImagePath.apply(NE), getImagePath.apply(N)));
			executeSystemCommand(
					constructAppendHCommand(getImagePath.apply(SW), getImagePath.apply(SE), getImagePath.apply(S)));
			

			executeSystemCommand(constructChopPixelVerticalCommand(getImagePath.apply(N), "South"));
			executeSystemCommand(constructChopPixelVerticalCommand(getImagePath.apply(S), "North"));
			
			executeSystemCommand(constructAppendVCommand(getImagePath.apply(N), getImagePath.apply(S),
					northWestTile.toString().replace(separatedImageDir(baseImageNum), separatedImageDir(TMP_DIR))));

			executeSystemCommand("rm " + getImagePath.apply(NW));
			executeSystemCommand("rm " + getImagePath.apply(NE));
			executeSystemCommand("rm " + getImagePath.apply(SW));
			executeSystemCommand("rm " + getImagePath.apply(SE));
			executeSystemCommand("rm " + getImagePath.apply(N));
			executeSystemCommand("rm " + getImagePath.apply(S));
		});
	}

	private void execute(String command) {

		try {
			Process process = Runtime.getRuntime().exec(command);
			if (debug) {
				logProcess(process, process.getInputStream());
			}

			process.waitFor();

			int exitValue = process.exitValue();
			if (exitValue != 0) {
				System.out.println("Process failed!");
				logProcess(process, process.getErrorStream());
				System.exit(1);
			} else {
				logDebug("Process completed");
			}
		} catch (Exception e) {
			System.out.println(e);
		}
	}

	private void logProcess(Process process, InputStream inputStream) throws IOException {
		String line;
		BufferedReader input = new BufferedReader(new InputStreamReader(inputStream));
		while ((line = input.readLine()) != null) {
			logDebug(line);
		}
		input.close();
	}

	private String createTmpImagePath(String zoom, String dirNum, String imageNum, String direction) {
		return baseDir + File.separator + TMP_DIR + File.separator + zoom + File.separator + dirNum + File.separator
				+ imageNum + "_" + direction + EXTENSION_PNG + " ";
	}

	private String createFullImagePath(String baseImage, String zoom, String dirNum, String imageNum) {
		return baseDir + baseImage + File.separator + zoom + File.separator + dirNum + File.separator + imageNum
				+ EXTENSION_PNG + " ";
	}

	private String constructSimpleTrimCommand(Path inputImagePath, int baseImageNum, String direction) {

		String tmpImagePath = inputImagePath.toString().replace(separatedImageDir(baseImageNum),
				separatedImageDir(TMP_DIR));
		return "convert " + inputImagePath.toString() + " -trim " + " "
				+ tmpImagePath.replace(EXTENSION_PNG, direction + EXTENSION_PNG);
	}
	
	private String constructChopPixelVerticalCommand(String inputImagePath, String direction) {
		
		return "convert " + inputImagePath.toString() + " -gravity " + direction + " +repage -chop 0x2 " + inputImagePath.toString();
	}
	
	private String constructChopPixelHorizontalCommand(String inputImagePath, String direction) {
		
		return "convert " + inputImagePath.toString() + " -gravity " + direction + " +repage -chop 2x0 " + inputImagePath.toString();
	}

	private String constructOneSidedTrimCommand(String baseImage, String zoom, String dirNum, String imageNum,
			String direction, String splice) {

		String inputImagePath = createFullImagePath(baseImage, zoom, dirNum, imageNum);
		String tmpImagePath = createTmpImagePath(zoom, dirNum, imageNum, direction);
		return "convert " + inputImagePath + " -gravity " + direction + " -background white -splice " + splice
				+ " -background black -splice " + splice + " -trim +repage -chop " + splice + " " + tmpImagePath;
	}

	private String constructAppendVerticallyCommand(String zoom, String dirNum, String imageNum) {

		String southImagePath = createTmpImagePath(zoom, dirNum, imageNum, "South");
		String northImagePath = createTmpImagePath(zoom, dirNum, imageNum, "North");
		String inputImagePath = createFullImagePath(TMP_DIR, zoom, dirNum, imageNum);
		return constructAppendVCommand(northImagePath, southImagePath, inputImagePath);
	}

	private String constructAppendVCommand(String northImagePath, String southImagePath, String inputImagePath) {
		return "convert -append " + northImagePath + " " + southImagePath + " " + inputImagePath;
	}

	private String constructAppendHorizontallyCommand(String zoom, String dirNum, String imageNum) {

		String westImagePath = createTmpImagePath(zoom, dirNum, imageNum, "West");
		String eastImagePath = createTmpImagePath(zoom, dirNum, imageNum, "East");
		String inputImagePath = createFullImagePath(TMP_DIR, zoom, dirNum, imageNum);
		return constructAppendHCommand(westImagePath, eastImagePath, inputImagePath);
	}

	private String constructAppendHCommand(String westImagePath, String eastImagePath, String inputImagePath) {
		return "convert +append " + westImagePath + " " + eastImagePath + " " + inputImagePath;
	}

}
