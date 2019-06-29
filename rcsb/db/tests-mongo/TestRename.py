import glob
import os


def renameTests():
    tfL = glob.glob("*Tests.py")
    for tf in tfL:
        rf = "test" + tf.replace("Tests", "")
        cmd = "git mv -f %s %s" % (tf, rf)
        print(cmd)
        os.system(cmd)


if __name__ == "__main__":
    renameTests()
