package pt.quintans.ezSQL.orm.app.domain;

import java.util.Set;

public class Painting extends BaseDomain<Long> {	
	private String name;
	private Double price;

	private Artist artist;
	private Long artistId;
	private Image image;
	private Long imageFk;
	
	private Set<Gallery> galleries;
	
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Double getPrice() {
		return price;
	}

	public void setPrice(Double price) {
		this.price = price;
	}

	public Artist getArtist() {
		return artist;
	}

	public void setArtist(Artist artist) {
		this.artist = artist;
		if(artist != null)
			artistId = artist.getId();
		else
			artistId = null;
	}

	public Long getArtistId() {
        return artistId;
    }

    public void setArtistId(Long artistId) {
        this.artistId = artistId;
    }

    public Set<Gallery> getGalleries() {
		return galleries;
	}

	public void setGalleries(Set<Gallery> galleries) {
		this.galleries = galleries;
	}

	public Image getImage() {
        return image;
    }

    public void setImage(Image image) {
        this.image = image;
        if(image != null)
            imageFk = image.getId();
        else
            imageFk = null;    }

    public Long getImageFk() {
		return imageFk;
	}

	public void setImageFk(Long imageFk) {
		this.imageFk = imageFk;
	}

	@Override
	public String toString() {
		final int maxLen = 10;
		StringBuilder builder = new StringBuilder();
		builder.append("Painting [id=");
		builder.append(id);
		builder.append(", version=");
		builder.append(version);
		builder.append(", name=");
		builder.append(name);
		builder.append(", price=");
		builder.append(price);
		builder.append(", artist=");
		builder.append(artist);
		builder.append(", artistId=");
		builder.append(artistId);
		builder.append(", imageFk=");
		builder.append(imageFk);
        builder.append(", image=");
        builder.append(image);
		builder.append(", galleries=");
		builder.append(galleries != null ? toString(galleries, maxLen) : null);
		builder.append("]");
		return builder.toString();
	}



}
